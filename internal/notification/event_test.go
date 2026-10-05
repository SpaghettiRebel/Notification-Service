package notification

import (
	"strings"
	"testing"
)

const validEvent = `{"event_id":"event-1","event_type":"birthday","channel":"tg",` +
	`"msg_text":"Поздравляем с днём рождения!","created_at":"2026-07-30T15:00:00+00:00"}`

func TestParseEventAcceptsValidEvent(t *testing.T) {
	ev, err := ParseEvent([]byte(validEvent))
	if err != nil {
		t.Fatalf("ParseEvent() error = %v", err)
	}
	if ev.EventID != "event-1" || ev.Channel != "tg" || ev.CreatedAt.IsZero() {
		t.Errorf("ParseEvent() = %+v", ev)
	}
}

func TestParseEventRejectsWhatPydanticRejected(t *testing.T) {
	for name, raw := range map[string]string{
		"extra field":         strings.Replace(validEvent, `"channel"`, `"extra":1,"channel"`, 1),
		"field in other case": strings.Replace(validEvent, `"event_id"`, `"EVENT_ID"`, 1),
		"missing created_at":  `{"event_id":"event-1","event_type":"birthday","channel":"tg","msg_text":"Поздравляем с днём рождения!"}`,
		"naive created_at":    strings.Replace(validEvent, "+00:00", "", 1),
		"null msg_text":       strings.Replace(validEvent, `"Поздравляем с днём рождения!"`, "null", 1),
		"9 characters":        strings.Replace(validEvent, `"Поздравляем с днём рождения!"`, `"Поздравля"`, 1),
		"unknown channel":     strings.Replace(validEvent, `"tg"`, `"fax"`, 1),
		"unknown event type":  strings.Replace(validEvent, `"birthday"`, `"easter"`, 1),
		"not an object":       `[]`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseEvent([]byte(raw)); err == nil {
				t.Errorf("ParseEvent() accepted %s", raw)
			}
		})
	}
}
