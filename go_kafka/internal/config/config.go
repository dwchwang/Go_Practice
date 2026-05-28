package config

import (
	"os"
	"strings"
)

type Config struct {
	PostgresDSN  string
	RedisAddr    string
	ServerPort   string
	KafkaBrokers []string
}

func Load() *Config {
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
