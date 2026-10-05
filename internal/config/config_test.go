package config

import "testing"

func validWorker() Worker {
	return Worker{
		Kafka:                       Kafka{BootstrapServers: "kafka:9092", DeliveryTimeoutMs: 10_000},
		KafkaMaxPollIntervalMs:      180_000,
		ProcessingLeaseSeconds:      60,
		SenderMaxAttempts:           3,
		SenderAttemptTimeoutSeconds: 10,
		SenderRetryMinSeconds:       1,
		SenderRetryMaxSeconds:       5,
	}
}

func TestWorkerValidation(t *testing.T) {
	if err := validWorker().validate(); err != nil {
		t.Fatalf("default settings rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Worker){
		"zero attempts":              func(c *Worker) { c.SenderMaxAttempts = 0 },
		"negative retry delay":       func(c *Worker) { c.SenderRetryMinSeconds = -1 },
		"retry min above max":        func(c *Worker) { c.SenderRetryMinSeconds = 6 },
		"lease shorter than retries": func(c *Worker) { c.ProcessingLeaseSeconds = 52 },
		"poll interval within lease": func(c *Worker) { c.KafkaMaxPollIntervalMs = 60_000 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validWorker()
			mutate(&cfg)
			if err := cfg.validate(); err == nil {
				t.Error("validate() accepted invalid settings")
			}
		})
	}
}

func TestDatabaseURLEscapesCredentials(t *testing.T) {
	cfg := Worker{DBHost: "db", DBPort: 5432, DBUser: "user", DBPass: "p@ss word", DBName: "notification_db"}

	if got, want := cfg.DatabaseURL(), "postgres://user:p%40ss%20word@db:5432/notification_db"; got != want {
		t.Errorf("DatabaseURL() = %q, want %q", got, want)
	}
}
