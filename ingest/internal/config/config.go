package config

import (
	"os"
	"strings"
)

// Config holds all runtime configuration for the ingest server.
// Values are populated from environment variables with sensible defaults.
type Config struct {
	ServerAddr   string
	KafkaBrokers []string
	KafkaTopic   string
}

func Load() Config {
	return Config{
		ServerAddr:   getEnv("SERVER_ADDR", ":4317"),
		KafkaBrokers: splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "otel.metrics"),
	}
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
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
