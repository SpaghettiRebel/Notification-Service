package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/SpaghettiRebel/Notification-Service/internal/notification"
)

// SendingMessage is the request body of POST /message/birthday and /message/christmas.
type SendingMessage struct {
	MsgType string `json:"msg_type"`
	MsgText string `json:"msg_text"`
}

// FieldError is one entry of a 422 response, in the shape FastAPI uses, so
// clients that parse detail[].loc keep working.
type FieldError struct {
	Loc  []string `json:"loc"`
	Msg  string   `json:"msg"`
	Type string   `json:"type"`
}

func (m SendingMessage) Validate() []FieldError {
	var errs []FieldError
	if !slices.Contains(notification.Channels, m.MsgType) {
		errs = append(errs, FieldError{
			Loc:  []string{"body", "msg_type"},
			Msg:  "Input should be one of: " + strings.Join(notification.Channels, ", "),
			Type: "enum",
		})
	}
	return append(errs, checkLength([]string{"body", "msg_text"}, m.MsgText, 10, 400)...)
}

func validateIdempotencyKey(key string) []FieldError {
	loc := []string{"header", "Idempotency-Key"}
	if key == "" {
		return []FieldError{{Loc: loc, Msg: "Field required", Type: "missing"}}
	}
	return checkLength(loc, key, 8, 128)
}

// checkLength counts characters, not bytes, as pydantic does.
func checkLength(loc []string, value string, lo, hi int) []FieldError {
	switch n := utf8.RuneCountInString(value); {
	case n < lo:
		return []FieldError{{Loc: loc, Msg: fmt.Sprintf("String should have at least %d characters", lo), Type: "string_too_short"}}
	case n > hi:
		return []FieldError{{Loc: loc, Msg: fmt.Sprintf("String should have at most %d characters", hi), Type: "string_too_long"}}
	}
	return nil
}

func decodeError(err error) FieldError {
	var typeErr *json.UnmarshalTypeError
	switch {
	case errors.Is(err, io.EOF):
		return FieldError{Loc: []string{"body"}, Msg: "Field required", Type: "missing"}
	case errors.As(err, &typeErr) && typeErr.Field != "":
		kind := typeErr.Type.String()
		return FieldError{Loc: []string{"body", typeErr.Field}, Msg: "Input should be a valid " + kind, Type: kind + "_type"}
	}
	return FieldError{Loc: []string{"body"}, Msg: "JSON decode error: " + err.Error(), Type: "json_invalid"}
}
