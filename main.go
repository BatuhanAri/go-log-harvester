package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
)

// Config yapısı: Tüm ayarlar tek bir yerde
type Config struct {
	ClickHouseAddr string
	ClickHouseDB   string
	ClickHouseUser string
	ClickHousePass string
	NatsURL        string
}

// LogData yapısı (Değişmedi)
type LogData struct {
	Service string `json:"service"`
	Level   string `json:"level"`
	Msg     string `json:"msg"`
	TS      string `json:"ts"`
}

const (
	BatchSize     = 1000
	FlushInterval = 5 * time.Second
)

// Ortam değişkenlerini yükleyen fonksiyon
func LoadConfig() Config {
	// .env dosyasını yüklemeyi dene (Prod ortamında dosya olmayabilir, hata yok sayılabilir)
	_ = godotenv.Load()

	return Config{
		ClickHouseAddr: getEnv("CLICKHOUSE_ADDR", "127.0.0.1:9000"),
		ClickHouseDB:   getEnv("CLICKHOUSE_DB", "default"),
		ClickHouseUser: getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePass: getEnv("CLICKHOUSE_PASSWORD", ""), // Varsayılan boş olmamalı aslında
		NatsURL:        getEnv("NATS_URL", nats.DefaultURL),
	}
}

// Yardımcı fonksiyon: Değişken yoksa varsayılanı dön
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func main() {
	// 1. Ayarları Yükle
	cfg := LoadConfig()

	// 2. ClickHouse Bağlantısı (Config'den)
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
	
	// Bağlantıyı test et (Ping)
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatalf("ClickHouse sunucusuna ulaşılamıyor: %v", err)
	}

	initDB(conn)

	// 3. NATS Bağlantısı (Config'den)
	nc, err := nats.Connect(cfg.NatsURL)
	if err != nil {
		log.Fatal("NATS bağlanamadı:", err)
	}
	defer nc.Close()

	// ... (Geri kalan kod aynı: Buffer, Channel, Loop) ...
	// Kodun geri kalanını aynen koruyabilirsin, sadece mantığı değiştirdik.
	// Aşağıya kısa versiyonunu ekliyorum:
	
	logChannel := make(chan LogData, 2000)
	_, _ = nc.Subscribe("logs.*", func(m *nats.Msg) {
		var data LogData
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

// Batch Processor'ı ana fonksiyondan ayırdım temiz görünsün diye
func runBatchProcessor(conn clickhouse.Conn, ch <-chan LogData) {
	batch := make([]LogData, 0, BatchSize)
	ticker := time.NewTicker(FlushInterval)

	for {
		select {
		case logEntry := <-ch:
			batch = append(batch, logEntry)
			if len(batch) >= BatchSize {
				saveToClickHouse(conn, batch)
				batch = make([]LogData, 0, BatchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				saveToClickHouse(conn, batch)
				batch = make([]LogData, 0, BatchSize)
			}
		}
	}
}

// (initDB ve saveToClickHouse fonksiyonları önceki ile aynı kalacak)
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

func saveToClickHouse(conn clickhouse.Conn, logs []LogData) {
	ctx := context.Background()
	batch, _ := conn.PrepareBatch(ctx, "INSERT INTO app_logs")
	for _, l := range logs {
		t, _ := time.Parse(time.RFC3339, l.TS)
		batch.Append(t, l.Service, l.Level, l.Msg)
	}
	batch.Send()
	fmt.Printf("%d log güvenli şekilde kaydedildi.\n", len(logs))
}