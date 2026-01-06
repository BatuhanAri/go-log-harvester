package config

import (
	"fmt"
	"os"
	"strings"

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

// Load loads configuration from environment variables.
// If envFile is empty, it attempts to load ".env" as an optional local-dev convenience.
// If envFile is provided, failing to load it returns an error.
func Load(envFile string) (Config, error) {
	if err := loadDotEnv(envFile); err != nil {
		return Config{}, err
	}

	cfg := Config{
		ClickHouseAddr: getEnv("CLICKHOUSE_ADDR", "127.0.0.1:9000"),
		ClickHouseDB:   getEnv("CLICKHOUSE_DB", "default"),
		ClickHouseUser: getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePass: getEnv("CLICKHOUSE_PASSWORD", ""),
		NatsURL:        getEnv("NATS_URL", nats.DefaultURL),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	var errs []string

	// Basic sanity checks
	if strings.TrimSpace(c.ClickHouseAddr) == "" {
		errs = append(errs, "CLICKHOUSE_ADDR is empty")
	}
	if strings.TrimSpace(c.ClickHouseDB) == "" {
		errs = append(errs, "CLICKHOUSE_DB is empty")
	}
	if strings.TrimSpace(c.ClickHouseUser) == "" {
		errs = append(errs, "CLICKHOUSE_USER is empty")
	}
	if strings.TrimSpace(c.NatsURL) == "" {
		errs = append(errs, "NATS_URL is empty")
	}

	// You can optionally enforce password presence in non-dev:
	// if strings.TrimSpace(c.ClickHousePass) == "" { errs = append(errs, "CLICKHOUSE_PASSWORD is empty") }

	if len(errs) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(errs, "; "))
	}
	return nil
}

func loadDotEnv(envFile string) error {
	// If explicit env file is provided, treat failure as error.
	if strings.TrimSpace(envFile) != "" {
		if err := godotenv.Load(envFile); err != nil {
			return fmt.Errorf("failed to load env file %q: %w", envFile, err)
		}
		return nil
	}

	// Optional default .env (do not fail if missing).
	// This prevents hiding real issues when user explicitly asked for a file.
	_ = godotenv.Load(".env")
	return nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
