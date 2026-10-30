CREATE TABLE IF NOT EXISTS events (
  id uuid PRIMARY KEY,
  album_id bigint NOT NULL,
  request jsonb NOT NULL,
  idempotency_key text UNIQUE,
  data jsonb,
  fetch_status text NOT NULL DEFAULT 'pending',
  delivery_status text NOT NULL DEFAULT 'pending',
  fetch_attempts integer NOT NULL DEFAULT 0,
  delivery_attempts integer NOT NULL DEFAULT 0,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
  id bigserial PRIMARY KEY,
  event_id uuid NOT NULL REFERENCES events(id),
  stage text NOT NULL,
  UNIQUE(event_id, stage)
);
