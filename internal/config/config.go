package config

import "os"

type Config struct {
	RedisAddr   string
	DatabaseDSN string
	ServerAddr  string
}

func Load() Config {
	return Config{
		RedisAddr: getEnv("REDIS_ADDR", "localhost:6379"),
		DatabaseDSN: getEnv(
			"DATABASE_DSN",
			"host=localhost user=ecommerce password=ecommerce dbname=mini_ecommerce port=5432 sslmode=disable TimeZone=Asia/Ho_Chi_Minh",
		),
		ServerAddr: getEnv("SERVER_ADDR", ":8080"),
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
