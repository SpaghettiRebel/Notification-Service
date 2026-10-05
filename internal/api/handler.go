// Package api implements the public HTTP API that turns greeting requests into Kafka events.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
)

// HealthChecker reports whether Kafka is reachable.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

const maxBodyBytes = 64 << 10

type publishFunc func(ctx context.Context, msg SendingMessage, idempotencyKey string) (string, error)

// NewHandler wires the routes of the public API.
func NewHandler(publisher *NotificationPublisher, health HealthChecker) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /message/birthday", sendHandler(publisher.PublishBirthday))
	mux.HandleFunc("POST /message/christmas", sendHandler(publisher.PublishChristmas))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := health.Ping(r.Context()); err != nil {
			slog.ErrorContext(r.Context(), "kafka readiness check failed", "error", err)
			writeDetail(w, http.StatusServiceUnavailable, "Kafka is unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	registerDocs(mux)
	return mux
}

func sendHandler(publish publishFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		errs := validateIdempotencyKey(key)

		var msg SendingMessage
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&msg); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeDetail(w, http.StatusRequestEntityTooLarge, "Request body too large")
				return
			}
			errs = append(errs, decodeError(err))
		} else {
			errs = append(errs, msg.Validate()...)
		}
		if len(errs) > 0 {
			writeJSON(w, http.StatusUnprocessableEntity, map[string][]FieldError{"detail": errs})
			return
		}

		eventID, err := publish(r.Context(), msg, key)
		var notAllowed *ChannelNotAllowedError
		switch {
		case errors.As(err, &notAllowed):
			writeDetail(w, http.StatusBadRequest, notAllowed.Detail)
		case err != nil:
			slog.ErrorContext(r.Context(), "failed to publish notification", "error", err)
			writeDetail(w, http.StatusInternalServerError, "Internal Server Error")
		default:
			writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted", "event_id": eventID})
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeDetail(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}
