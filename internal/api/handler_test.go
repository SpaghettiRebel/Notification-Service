package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeHealth struct{ err error }

func (f fakeHealth) Ping(context.Context) error { return f.err }

func serve(handler http.Handler, method, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestSendEndpoints(t *testing.T) {
	const key = "request-key-123"
	for _, tc := range []struct {
		name, path, key, body string
		wantStatus            int
		wantBody              string
	}{
		{"accepted", "/message/birthday", key, `{"msg_type":"tg","msg_text":"Happy birthday to you!"}`,
			http.StatusAccepted, `"status":"accepted"`},
		{"birthday email", "/message/birthday", key, `{"msg_type":"email","msg_text":"Happy birthday to you!"}`,
			http.StatusBadRequest, "You cannot send an Email with birthday message"},
		{"christmas sms", "/message/christmas", key, `{"msg_type":"sms","msg_text":"Merry Christmas to you!"}`,
			http.StatusBadRequest, "You cannot send an SMS with Christmas message"},
		{"missing key", "/message/christmas", "", `{"msg_type":"tg","msg_text":"Merry Christmas to you!"}`,
			http.StatusUnprocessableEntity, `"loc":["header","Idempotency-Key"],"msg":"Field required"`},
		{"short key", "/message/christmas", "short", `{"msg_type":"tg","msg_text":"Merry Christmas to you!"}`,
			http.StatusUnprocessableEntity, `"loc":["header","Idempotency-Key"]`},
		{"short text", "/message/christmas", key, `{"msg_type":"tg","msg_text":"short"}`,
			http.StatusUnprocessableEntity, `"type":"string_too_short"`},
		{"unknown channel", "/message/christmas", key, `{"msg_type":"fax","msg_text":"Merry Christmas to you!"}`,
			http.StatusUnprocessableEntity, `"loc":["body","msg_type"]`},
		{"wrong type", "/message/christmas", key, `{"msg_type":"tg","msg_text":42}`,
			http.StatusUnprocessableEntity, `"loc":["body","msg_text"],"msg":"Input should be a valid string"`},
		{"broken JSON", "/message/christmas", key, `{`,
			http.StatusUnprocessableEntity, `"type":"json_invalid"`},
		{"empty body", "/message/christmas", key, ``,
			http.StatusUnprocessableEntity, `"loc":["body"],"msg":"Field required"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := NewHandler(NewNotificationPublisher(&fakeEvents{}), fakeHealth{})

			rec := serve(handler, http.MethodPost, tc.path, tc.key, tc.body)

			if rec.Code != tc.wantStatus || !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Errorf("got %d %s, want %d containing %s", rec.Code, rec.Body, tc.wantStatus, tc.wantBody)
			}
		})
	}
}

func TestServiceEndpoints(t *testing.T) {
	healthy := NewHandler(NewNotificationPublisher(&fakeEvents{}), fakeHealth{})
	broken := NewHandler(NewNotificationPublisher(&fakeEvents{}), fakeHealth{errors.New("broker down")})
	for _, tc := range []struct {
		name       string
		handler    http.Handler
		path       string
		wantStatus int
		wantBody   string
	}{
		{"live", broken, "/live", http.StatusOK, `{"status":"ok"}`},
		{"health ok", healthy, "/health", http.StatusOK, `{"status":"ok"}`},
		{"health without kafka", broken, "/health", http.StatusServiceUnavailable, "Kafka is unavailable"},
		{"swagger ui", healthy, "/docs", http.StatusOK, "SwaggerUIBundle"},
		{"redoc", healthy, "/redoc", http.StatusOK, "redoc.standalone.js"},
		{"openapi spec", healthy, "/openapi.yaml", http.StatusOK, "/message/birthday:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := serve(tc.handler, http.MethodGet, tc.path, "", "")

			if rec.Code != tc.wantStatus || !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Errorf("got %d %s, want %d containing %s", rec.Code, rec.Body, tc.wantStatus, tc.wantBody)
			}
		})
	}
}
