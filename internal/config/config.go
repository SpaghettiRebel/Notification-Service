// Package config loads service settings from the environment and an optional .env file.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Kafka holds the settings both services need to publish events.
type Kafka struct {
	BootstrapServers  string `env:"KAFKA_BOOTSTRAP_SERVERS" envDefault:"kafka:9092"`
	DeliveryTimeoutMs int    `env:"KAFKA_DELIVERY_TIMEOUT_MS" envDefault:"10000"`
}

func (k Kafka) Brokers() []string { return strings.Split(k.BootstrapServers, ",") }

func (k Kafka) DeliveryTimeout() time.Duration {
	return time.Duration(k.DeliveryTimeoutMs) * time.Millisecond
}

// API configures the public HTTP API.
type API struct {
	Kafka
	Host string `env:"APP_HOST" envDefault:"0.0.0.0"`
	Port int    `env:"APP_PORT" envDefault:"8001"`
}

// Worker configures the notification worker.
type Worker struct {
	Kafka
	DBHost string `env:"DB_HOST,required"`
	DBPort int    `env:"DB_PORT,required"`
	DBUser string `env:"DB_USER,required"`
	DBPass string `env:"DB_PASS,required"`
	DBName string `env:"DB_NAME,required"`

	KafkaMaxPollIntervalMs      int     `env:"KAFKA_MAX_POLL_INTERVAL_MS" envDefault:"180000"`
	ProcessingLeaseSeconds      int     `env:"PROCESSING_LEASE_SECONDS" envDefault:"60"`
	SenderMaxAttempts           int     `env:"SENDER_MAX_ATTEMPTS" envDefault:"3"`
	SenderAttemptTimeoutSeconds float64 `env:"SENDER_ATTEMPT_TIMEOUT_SECONDS" envDefault:"10"`
	SenderRetryMinSeconds       float64 `env:"SENDER_RETRY_MIN_SECONDS" envDefault:"1"`
	SenderRetryMaxSeconds       float64 `env:"SENDER_RETRY_MAX_SECONDS" envDefault:"5"`
}

func LoadAPI() (API, error) {
	var cfg API
	if err := load(&cfg); err != nil {
		return API{}, err
	}
	if cfg.DeliveryTimeoutMs <= 0 {
		return API{}, errors.New("KAFKA_DELIVERY_TIMEOUT_MS must be greater than 0")
	}
	return cfg, nil
}

func LoadWorker() (Worker, error) {
	var cfg Worker
	if err := load(&cfg); err != nil {
		return Worker{}, err
	}
	if err := cfg.validate(); err != nil {
		return Worker{}, err
	}
	return cfg, nil
}

// load reads an optional .env file from the working directory first; variables
// that are already set in the environment take precedence over it.
func load(cfg any) error {
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("load .env: %w", err)
	}
	if err := env.Parse(cfg); err != nil {
		return fmt.Errorf("parse environment: %w", err)
	}
	return nil
}

// validate rejects settings under which a lease could expire while its event
// is still being processed, so a second worker could pick the event up.
func (c Worker) validate() error {
	positive := []struct {
		name  string
		value float64
	}{
		{"KAFKA_DELIVERY_TIMEOUT_MS", float64(c.DeliveryTimeoutMs)},
		{"KAFKA_MAX_POLL_INTERVAL_MS", float64(c.KafkaMaxPollIntervalMs)},
		{"PROCESSING_LEASE_SECONDS", float64(c.ProcessingLeaseSeconds)},
		{"SENDER_MAX_ATTEMPTS", float64(c.SenderMaxAttempts)},
		{"SENDER_ATTEMPT_TIMEOUT_SECONDS", c.SenderAttemptTimeoutSeconds},
	}
	for _, p := range positive {
		if p.value <= 0 {
			return fmt.Errorf("%s must be greater than 0", p.name)
		}
	}
	if c.SenderRetryMinSeconds < 0 || c.SenderRetryMaxSeconds < 0 {
		return errors.New("sender retry delays must not be negative")
	}
	if c.SenderRetryMinSeconds > c.SenderRetryMaxSeconds {
		return errors.New("SENDER_RETRY_MIN_SECONDS must not exceed SENDER_RETRY_MAX_SECONDS")
	}
	maxProcessingSeconds := float64(c.SenderMaxAttempts)*c.SenderAttemptTimeoutSeconds +
		float64(c.SenderMaxAttempts-1)*c.SenderRetryMaxSeconds +
		float64(c.DeliveryTimeoutMs)/1000 + 2
	if float64(c.ProcessingLeaseSeconds) <= maxProcessingSeconds {
		return errors.New("PROCESSING_LEASE_SECONDS must exceed the maximum sender retry and DLQ delivery duration")
	}
	if c.KafkaMaxPollIntervalMs <= c.ProcessingLeaseSeconds*1000 {
		return errors.New("KAFKA_MAX_POLL_INTERVAL_MS must exceed the processing lease")
	}
	return nil
}

func (c Worker) DatabaseURL() string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.DBUser, c.DBPass),
		Host:   net.JoinHostPort(c.DBHost, strconv.Itoa(c.DBPort)),
		Path:   "/" + c.DBName,
	}
	return u.String()
}

func (c Worker) ProcessingLease() time.Duration {
	return time.Duration(c.ProcessingLeaseSeconds) * time.Second
}

func (c Worker) MaxPollInterval() time.Duration {
	return time.Duration(c.KafkaMaxPollIntervalMs) * time.Millisecond
}

func (c Worker) AttemptTimeout() time.Duration { return seconds(c.SenderAttemptTimeoutSeconds) }
func (c Worker) RetryMin() time.Duration       { return seconds(c.SenderRetryMinSeconds) }
func (c Worker) RetryMax() time.Duration       { return seconds(c.SenderRetryMaxSeconds) }

func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }
