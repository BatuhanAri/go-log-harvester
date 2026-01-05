package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
)

// Göndereceğimiz veri formatı
type LogData struct {
	Service string `json:"service"`
	Level   string `json:"level"`
	Msg     string `json:"msg"`
	TS      string `json:"ts"`
}

func main() {
	// NATS'a bağlan (Localhost)
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	services := []string{"auth-service", "payment-api", "user-db", "frontend"}
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	messages := []string{"Bağlantı koptu", "Kullanıcı giriş yaptı", "Ödeme alındı", "Cache temizlendi", "Timeout oluştu"}

	fmt.Println("Log saldırısı başlıyor... (Çıkmak için Ctrl+C)")

	for {
		// Rastgele veri üret
		logEntry := LogData{
			Service: services[rand.Intn(len(services))],
			Level:   levels[rand.Intn(len(levels))],
			Msg:     messages[rand.Intn(len(messages))],
			TS:      time.Now().Format(time.RFC3339),
		}

		// JSON'a çevir
		data, _ := json.Marshal(logEntry)

		// NATS'a fırlat (Konu: logs.herhangi-bir-sey)
		subject := "logs.test"
		nc.Publish(subject, data)

		fmt.Printf("[GÖNDERİLDİ] %s -> %s\n", subject, logEntry.Msg)

		// Biraz bekle (Çok hızlı akmasın, takip edelim)
		time.Sleep(500 * time.Millisecond)
	}
}
