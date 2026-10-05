-- IF NOT EXISTS lets the worker adopt a database whose schema was created by
-- the Alembic migrations of the Python version.

-- +goose Up
-- +goose StatementBegin
DO $$
BEGIN
    CREATE TYPE notification_status AS ENUM ('pending', 'completed', 'failed');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END
$$;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS notification_log (
    event_id   VARCHAR             NOT NULL PRIMARY KEY,
    status     notification_status NOT NULL,
    updated_at TIMESTAMPTZ         NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- +goose Down
DROP TABLE IF EXISTS notification_log;
DROP TYPE IF EXISTS notification_status;
