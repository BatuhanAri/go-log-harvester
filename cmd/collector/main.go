package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BatuhanAri/go-log-harvester/internal/config"
	"github.com/BatuhanAri/go-log-harvester/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nats-io/nats.go"
)

const (
	BatchSize     = 1000
	FlushInterval = 5 * time.Second
	ChanBuffer    = 5000
)

func main() {
	// ---- Flags (prod’da şart)
	envFile := flag.String("env", "", "optional .env path (if set, load failure is fatal)")
	subject := flag.String("subject", "logs.*", "NATS subject to subscribe")
	flag.Parse()

	// ---- Root context + graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ---- Load config (NEW API)
	cfg, err := config.Load(*envFile)
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}

	// ---- ClickHouse connect + ping
	conn, err := openClickHouse(cfg)
	if err != nil {
		log.Fatalf("clickhouse connect failed: %v", err)
	}
	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("clickhouse ping failed: %v", err)
	}
	if err := initDB(ctx, conn); err != nil {
		log.Fatalf("clickhouse init failed: %v", err)
	}

	// ---- NATS connect
	nc, err := nats.Connect(
		cfg.NatsURL,
		nats.Name("go-log-harvester/collector"),
		nats.Timeout(5*time.Second),
		nats.ReconnectWait(1*time.Second),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, e error) {
			log.Printf("NATS disconnected: %v", e)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS reconnected: %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Printf("NATS connection closed")
		}),
	)
	if err != nil {
		log.Fatalf("nats connect failed: %v", err)
	}
	defer nc.Close()

	// ---- Data pipeline
	logCh := make(chan models.LogData, ChanBuffer)
	var dropped uint64

	// Subscribe
	sub, err := nc.Subscribe(*subject, func(m *nats.Msg) {
		var data models.LogData
		if err := json.Unmarshal(m.Data, &data); err != nil {
			// Invalid payload: drop
			return
		}
		if data.TS == "" {
			data.TS = time.Now().Format(time.RFC3339)
		}

		// Backpressure policy: channel full => drop + count
		select {
		case logCh <- data:
		default:
			atomic.AddUint64(&dropped, 1)
		}
	})
	if err != nil {
		log.Fatalf("nats subscribe failed: %v", err)
	}
	defer func() {
		_ = sub.Unsubscribe()
	}()

	// NATS callback'lerin çalışması için flush iyi pratik
	if err := nc.FlushTimeout(2 * time.Second); err != nil {
		log.Fatalf("nats flush failed: %v", err)
	}

	log.Printf("Collector started. subject=%s batch=%d flush=%s chan=%d",
		*subject, BatchSize, FlushInterval, ChanBuffer)

	// ---- Run batch processor
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runBatchProcessor(ctx, conn, logCh, &dropped)
	}()

	// ---- Wait for shutdown signal
	<-ctx.Done()
	log.Printf("Shutdown signal received, draining...")

	// Stop receiving new messages quickly
	_ = sub.Unsubscribe()

	// Drain NATS
	_ = nc.Drain()

	// Close channel after short grace period to let callbacks finish
	closeAfter := time.NewTimer(1 * time.Second)
	select {
	case <-closeAfter.C:
		close(logCh)
	case <-ctx.Done():
		// already canceled; proceed
		close(logCh)
	}
	closeAfter.Stop()

	wg.Wait()
	log.Printf("Collector stopped. dropped=%d", atomic.LoadUint64(&dropped))
}

// --- ClickHouse open with sane options
func openClickHouse(cfg config.Config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePass,
		},
		DialTimeout: 5 * time.Second,
		// Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// --- Batch processor: flush by size or interval, flush remaining on exit
func runBatchProcessor(ctx context.Context, conn driver.Conn, ch <-chan models.LogData, dropped *uint64) {
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	batch := make([]models.LogData, 0, BatchSize)

	flush := func(logs []models.LogData) {
		if len(logs) == 0 {
			return
		}
		if err := saveToClickHouse(ctx, conn, logs); err != nil {
			log.Printf("clickhouse insert failed: %v (count=%d)", err, len(logs))
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush(batch)
			return

		case item, ok := <-ch:
			if !ok {
				flush(batch)
				return
			}
			batch = append(batch, item)
			if len(batch) >= BatchSize {
				flush(batch)
				batch = make([]models.LogData, 0, BatchSize)
			}

		case <-ticker.C:
			flush(batch)
			batch = make([]models.LogData, 0, BatchSize)

			// görünürlük (opsiyonel)
			d := atomic.LoadUint64(dropped)
			if d > 0 {
				log.Printf("stats: dropped=%d (channel backpressure)", d)
			}
		}
	}
}

func initDB(ctx context.Context, conn driver.Conn) error {
	query := `
CREATE TABLE IF NOT EXISTS app_logs (
	ts DateTime,
	service String,
	level String,
	msg String
) ENGINE = MergeTree()
ORDER BY ts
`
	return conn.Exec(ctx, query)
}

func saveToClickHouse(ctx context.Context, conn driver.Conn, logs []models.LogData) error {
	b, err := conn.PrepareBatch(ctx, "INSERT INTO app_logs (ts, service, level, msg)")
	if err != nil {
		return err
	}

	for _, l := range logs {
		t, err := parseTS(l.TS)
		if err != nil {

			t = time.Now()
		}

		if err := b.Append(t, l.Service, l.Level, l.Msg); err != nil {
			return err
		}
	}

	if err := b.Send(); err != nil {
		return err
	}

	log.Printf("%d logs inserted", len(logs))
	return nil
}

func parseTS(ts string) (time.Time, error) {
	if ts == "" {
		return time.Time{}, errors.New("empty ts")
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err == nil {
		return t, nil
	}
	// bazı kaynaklar RFC3339Nano basar
	t, err2 := time.Parse(time.RFC3339Nano, ts)
	if err2 == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid ts %q: %v", ts, err)
}
