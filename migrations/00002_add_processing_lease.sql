-- +goose Up
ALTER TABLE notification_log
    ADD COLUMN IF NOT EXISTS locked_until  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_error    TEXT;

-- +goose Down
ALTER TABLE notification_log
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS attempt_count,
    DROP COLUMN IF EXISTS locked_until;
