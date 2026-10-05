// Command publicapi accepts greeting requests over HTTP and publishes them to Kafka.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/SpaghettiRebel/Notification-Service/internal/api"
	"github.com/SpaghettiRebel/Notification-Service/internal/config"
	"github.com/SpaghettiRebel/Notification-Service/internal/kafka"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("public api stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI()
	if err != nil {
		return err
	}
	// The runtime image has no shell or curl, so the Docker healthcheck runs this binary.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		return healthcheck(cfg.Port)
	}

	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		return err
	}
	defer producer.Close()

	srv := &http.Server{
		Addr:              net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Handler:           api.NewHandler(api.NewNotificationPublisher(producer), producer),
		ReadHeaderTimeout: 5 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	slog.Info("public api listening", "addr", srv.Addr)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	slog.Info("public api shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func healthcheck(port int) error {
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://localhost:" + strconv.Itoa(port) + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("health check returned " + resp.Status)
	}
	return nil
}
