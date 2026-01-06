package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
)

type LogData struct {
	Service string `json:"service"`
	Level   string `json:"level"`
	Msg     string `json:"msg"`
	TS      string `json:"ts"`
}

func main() {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	services := []string{"auth-service", "payment-api", "user-db", "frontend"}
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	messages := []string{
		"Bağlantı koptu",
		"Kullanıcı giriş yaptı",
		"Ödeme alındı",
		"Cache temizlendi",
		"Timeout oluştu",
	}

	burstSize := 10000
	start := time.Now()

	for i := 0; i < burstSize; i++ {
		logEntry := LogData{
			Service: services[rand.Intn(len(services))],
			Level:   levels[rand.Intn(len(levels))],
			Msg:     messages[rand.Intn(len(messages))],
			TS:      time.Now().Format(time.RFC3339),
		}

		data, _ := json.Marshal(logEntry)
		nc.Publish("logs.test", data)
	}

	nc.Flush()

	fmt.Printf("✅ Burst tamamlandı: %d log, süre: %s\n",
		burstSize,
		time.Since(start),
	)
}
