package config

import (
	"os"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
)

type Config struct {
	ClickHouseAddr string
	ClickHouseDB   string
	ClickHouseUser string
	ClickHousePass string
	NatsURL        string
}

func LoadConfig() Config {
	_ = godotenv.Load() 
    _ = godotenv.Load("../.env") 

	return Config{
		ClickHouseAddr: getEnv("CLICKHOUSE_ADDR", "127.0.0.1:9000"),
		ClickHouseDB:   getEnv("CLICKHOUSE_DB", "default"),
		ClickHouseUser: getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePass: getEnv("CLICKHOUSE_PASSWORD", ""),
		NatsURL:        getEnv("NATS_URL", nats.DefaultURL),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}