// Command worker consumes notification events from Kafka and delivers them.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SpaghettiRebel/Notification-Service/internal/config"
	"github.com/SpaghettiRebel/Notification-Service/internal/kafka"
	"github.com/SpaghettiRebel/Notification-Service/internal/sender"
	"github.com/SpaghettiRebel/Notification-Service/internal/storage"
	"github.com/SpaghettiRebel/Notification-Service/internal/worker"
	"github.com/SpaghettiRebel/Notification-Service/migrations"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("worker shut down gracefully")
}

func run() error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL())
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close()
	if err := migrations.Up(ctx, pool); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		return err
	}
	defer consumer.Close()

	processor := worker.NewProcessor(producer, sender.Stub{}, storage.NewRepository(pool), worker.Settings{
		ProcessingLease: cfg.ProcessingLease(),
		MaxAttempts:     cfg.SenderMaxAttempts,
		AttemptTimeout:  cfg.AttemptTimeout(),
		RetryMin:        cfg.RetryMin(),
		RetryMax:        cfg.RetryMax(),
	})

	slog.Info("notification worker started")
	if err := consumer.Run(ctx, processor.Process); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
