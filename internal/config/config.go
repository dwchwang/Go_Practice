package config

import (
	"fmt"
	"os"
)

type Config struct {
	RedisAddr   string
	DatabaseDSN string
	ServerAddr  string
}

func Load() Config {
	// 1. Đọc các cấu hình đơn lẻ từ môi trường (hoặc dùng fallback nếu chạy ngoài Docker)
	// Khi chạy bằng go run trên máy host, dùng 127.0.0.1 để tránh bị trỏ nhầm sang Postgres local qua IPv6 localhost.
	// Khi chạy trong Docker, override DB_HOST=postgres và DB_PORT=5432.
	dbHost := getEnv("DB_HOST", "127.0.0.1")
	dbUser := getEnv("DB_USER", "postgres")
	dbPass := getEnv("DB_PASSWORD", "123456")
	dbName := getEnv("DB_NAME", "mini_ecommerce")

	// Khi chạy ngoài Docker (go run): kết nối qua port host mở ra ngoài (5433).
	// Khi chạy trong Docker: kết nối trực tiếp qua port nội bộ của container (5432).
	dbPort := getEnv("DB_PORT", "5433")

	// 2. Tự động gom thành chuỗi DSN hoàn chỉnh
	// Fix luôn vấn đề TimeZone bằng cách giữ chặt Asia/Ho_Chi_Minh
	defaultDSN := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=Asia/Ho_Chi_Minh",
		dbHost, dbUser, dbPass, dbName, dbPort,
	)

	return Config{
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		DatabaseDSN: getEnv("DATABASE_DSN", defaultDSN),
		ServerAddr:  getEnv("SERVER_ADDR", ":8080"),
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
