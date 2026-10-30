# Discogs webhook

Copy `.env.example` to `.env`, set a strong `WEBHOOK_TOKEN`, and run `docker compose up --build`. PostgreSQL migrations run before the API and worker start. Local alternative: install dependencies, run `npm run build`, `npm run migrate`, then `npm start` and `npm run worker` in separate terminals with PostgreSQL and Redis running.

Submit `POST /webhooks/discogs` with `Authorization: Bearer <WEBHOOK_TOKEN>`, `Content-Type: application/json`, and body `{"albumId":123}`. The response is `202 {"eventId":"..."}`. Optionally provide `Idempotency-Key`; the same key and album return the original event, while conflicting album IDs return 409. Retrieve persisted state and data through authenticated `GET /events/<eventId>`.

Events are committed to PostgreSQL before acknowledgement. The outbox dispatcher retries Redis handoffs. Fetch and delivery jobs have five attempts with exponential backoff, honoring Retry-After. Other upstream 4xx failures are terminal. Set `DISCOGS_TOKEN` optionally and provide an identifying `DISCOGS_USER_AGENT`. `DISCOGS_INTERVAL_MS` defaults to 3000.

Without `DESTINATION_URL`, results remain in PostgreSQL awaiting configuration. Configure the URL and optional `DESTINATION_TOKEN`, then restart the worker. Pending results are sent as POST JSON `{eventId,albumId,data}` with `Idempotency-Key: <eventId>`. Delivery is at least once: receivers must deduplicate. Delivery retries do not refetch data. Redirects are rejected. Events and BullMQ jobs are retained indefinitely; operational retention and manual failed-job retries must be managed by the operator.

`GET /health/live` checks the API process; `/health/ready` checks PostgreSQL, the dependency needed to accept durable events. Redis availability is reflected by worker error logs. Shut down with SIGINT/SIGTERM. Docker credentials are development defaults; configure production credentials and infrastructure separately.

If Compose reports that it cannot connect to `dockerDesktopLinuxEngine` or that the named pipe cannot be found, Docker Desktop's Linux engine is not running. Start Docker Desktop, wait until it reports that the engine is running, and confirm with `docker info`; then retry `docker compose up --build`. Make sure Docker Desktop is using Linux containers.

Validation: `npm run check` and `npm run build`. Integration validation requires PostgreSQL and Redis: submit an event, verify stored results, enable a mock destination and restart the worker, verify payload and idempotency header; repeat with transient errors and Redis outages. Automated integration tests are not yet included.

Validation: `npm run check` and `npm run build`. Integration validation requires PostgreSQL and Redis: submit an event, verify stored results, enable a mock destination and restart the worker, verify payload and idempotency header; repeat with transient errors and Redis outages. Automated integration tests are not yet included.
