// Package sender holds the delivery adapters for Telegram, email and SMS.
package sender

import (
	"context"
	"math/rand/v2"

	"github.com/SpaghettiRebel/Notification-Service/internal/notification"
)

// Stub stands in for the real providers: on average every fifth call fails
// with a retriable "server unavailable" error.
type Stub struct{}

func (Stub) SendTg(context.Context, notification.Event, string) error {
	return maybeFail(notification.ErrTgUnavailable)
}

func (Stub) SendEmail(context.Context, notification.Event, string) error {
	return maybeFail(notification.ErrSMTPUnavailable)
}

func (Stub) SendSMS(context.Context, notification.Event, string) error {
	return maybeFail(notification.ErrSMSUnavailable)
}

func maybeFail(err error) error {
	if rand.IntN(5) == 0 {
		return err
	}
	return nil
}
