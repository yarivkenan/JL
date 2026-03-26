package config

import (
	"os"
	"strconv"
	"strings"
)

// Config holds runtime configuration for the storage consumer.
type Config struct {
	KafkaBrokers  []string
	KafkaTopic    string
	ConsumerGroup string
	MaxRetries    int
	DatabaseURL   string
	StorageAddr   string
}

func Load() Config {
	return Config{
		KafkaBrokers:  splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:    getEnv("KAFKA_TOPIC", "otel.metrics"),
		ConsumerGroup: getEnv("CONSUMER_GROUP", "storage-consumers"),
		MaxRetries:    getEnvInt("MAX_RETRIES", 3),
		DatabaseURL:   getEnv("DATABASE_URL", "postgres://otel:otel@localhost:5432/otel_metrics"),
		StorageAddr:   getEnv("STORAGE_ADDR", ":8081"),
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
