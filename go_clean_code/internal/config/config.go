package config

import (
	"os"
	"strings"
	"sync"
)

type Config struct {
	PostgresDSN  string
	RedisAddr    string
	ServerPort   string
	KafkaBrokers []string
}

var (
	instance *Config
	once     sync.Once
)

// Get trả về instance Config duy nhất trong toàn process, thread-safe nhờ sync.Once.
func Get() *Config {
	once.Do(func() {
		instance = load()
	})
	return instance
}

func load() *Config {
	return &Config{
		PostgresDSN: getEnv(
			"POSTGRES_DSN",
			"host=localhost user=postgres password=123456 dbname=order_processing port=5433 sslmode=disable TimeZone=Asia/Bangkok",
		),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		ServerPort:   getEnv("SERVER_PORT", "3000"),
		KafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
