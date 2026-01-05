package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/BatuhanAri/go-log-harvester/internal/models"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
)

// Config yapısı: Bu servise özel ayarlar
type Config struct {
	ClickHouseAddr string
	ClickHouseDB   string
	ClickHouseUser string
	ClickHousePass string
	NatsURL        string
}

const (
	BatchSize     = 1000
	FlushInterval = 5 * time.Second
)

// Ortam değişkenlerini yükleyen fonksiyon
func LoadConfig() Config {
	_ = godotenv.Load()

	return Config{
		ClickHouseAddr: getEnv("CLICKHOUSE_ADDR", "127.0.0.1:9000"),
		ClickHouseDB:   getEnv("CLICKHOUSE_DB", "default"),
		ClickHouseUser: getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePass: getEnv("CLICKHOUSE_PASSWORD", ""),
		NatsURL:        getEnv("NATS_URL", nats.DefaultURL),
	}
}

// Yardımcı fonksiyon
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func main() {
	// Ayarları Yükle
	cfg := LoadConfig()

	// ClickHouse Bağlantısı
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePass,
		},
	})
	if err != nil {
		log.Fatalf("ClickHouse config hatası: %v", err)
	}

	// Bağlantıyı test et
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatalf("ClickHouse sunucusuna ulaşılamıyor: %v", err)
	}

	initDB(conn)

	// NATS Bağlantısı
	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		log.Fatal("NATS bağlanamadı:", err)
	}
	defer nc.Close()

	// Kanal artık models.LogData taşıyor
	logChannel := make(chan models.LogData, 2000)

	// Subscribe
	_, _ = nc.Subscribe("logs.*", func(m *nats.Msg) {
		var data models.LogData // models paketini kullanıyoruz
		if err := json.Unmarshal(m.Data, &data); err == nil {
			if data.TS == "" {
				data.TS = time.Now().Format(time.RFC3339)
			}
			logChannel <- data
		}
	})

	fmt.Println("Log Collector (Secure Mode) çalışıyor...")
	runBatchProcessor(conn, logChannel)
}

// Batch Processor
func runBatchProcessor(conn clickhouse.Conn, ch <-chan models.LogData) {
	batch := make([]models.LogData, 0, BatchSize) // Slice türü güncellendi
	ticker := time.NewTicker(FlushInterval)

	for {
		select {
		case logEntry := <-ch:
			batch = append(batch, logEntry)
			if len(batch) >= BatchSize {
				saveToClickHouse(conn, batch)
				batch = make([]models.LogData, 0, BatchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				saveToClickHouse(conn, batch)
				batch = make([]models.LogData, 0, BatchSize)
			}
		}
	}
}

func initDB(conn clickhouse.Conn) {
	query := `
	CREATE TABLE IF NOT EXISTS app_logs (
		ts DateTime,
		service String,
		level String,
		msg String
	) ENGINE = MergeTree()
	ORDER BY ts
	`
	conn.Exec(context.Background(), query)
}

func saveToClickHouse(conn clickhouse.Conn, logs []models.LogData) {
	ctx := context.Background()
	batch, _ := conn.PrepareBatch(ctx, "INSERT INTO app_logs")
	for _, l := range logs {
		t, _ := time.Parse(time.RFC3339, l.TS)
		batch.Append(t, l.Service, l.Level, l.Msg)
	}
	batch.Send()
	fmt.Printf("%d log güvenli şekilde kaydedildi.\n", len(logs))
}
