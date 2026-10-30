import express from 'express';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { pool, required, transaction } from './common.js';

const token = required('WEBHOOK_TOKEN');
const app = express();

app.use(express.json({ limit: '32kb' }));

app.get('/health/live', (_request, response) => {
  response.json({ ok: true });
});

app.get('/health/ready', async (_request, response) => {
  try {
    await pool.query('SELECT 1');
    response.json({ ok: true });
  } catch {
    response.status(503).json({ ok: false });
  }
});

app.use((request, response, next) => {
  const actual = Buffer.from(request.headers.authorization || '');
  const expected = Buffer.from(`Bearer ${token}`);

  if (
    actual.length !== expected.length ||
    !timingSafeEqual(actual, expected)
  ) {
    response.sendStatus(401);
    return;
  }

  next();
});

app.post('/webhooks/discogs', async (request, response, next) => {
  try {
    const albumId = request.body?.albumId;
    if (!Number.isSafeInteger(albumId) || albumId <= 0) {
      response.status(400).json({
        error: 'albumId must be a positive safe integer',
      });
      return;
    }

    const key = request.get('Idempotency-Key');
    const result = await transaction(async (client) => {
      const id = randomUUID();
      const inserted = await client.query(
        `INSERT INTO events(id, album_id, request, idempotency_key)
         VALUES($1, $2, $3, $4)
         ON CONFLICT(idempotency_key) DO NOTHING
         RETURNING id`,
        [id, albumId, request.body, key || null],
      );

      if (!inserted.rowCount) {
        const old = (
          await client.query(
            'SELECT id, album_id FROM events WHERE idempotency_key=$1',
            [key],
          )
        ).rows[0];

        return {
          eventId: old.id,
          conflict: Number(old.album_id) !== albumId,
        };
      }

      await client.query(
        "INSERT INTO outbox(event_id, stage) VALUES($1, 'fetch')",
        [id],
      );
      return { eventId: id, conflict: false };
    });

    response.status(result.conflict ? 409 : 202).json(
      result.conflict
        ? { error: 'Idempotency key conflict' }
        : { eventId: result.eventId },
    );
  } catch (error) {
    next(error);
  }
});

app.get('/events/:id', async (request, response, next) => {
  try {
    if (!/^[0-9a-f-]{36}$/i.test(request.params.id)) {
      response.sendStatus(404);
      return;
    }

    const result = await pool.query('SELECT * FROM events WHERE id=$1', [
      request.params.id,
    ]);
    if (!result.rowCount) {
      response.sendStatus(404);
      return;
    }
    response.json(result.rows[0]);
  } catch (error) {
    next(error);
  }
});

app.use((
  error: Error,
  _request: express.Request,
  response: express.Response,
  _next: express.NextFunction,
) => {
  console.error({ error: error.message });
  response.status(500).json({ error: 'Internal server error' });
});

const server = app.listen(Number(process.env.PORT || 3000));

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    server.close(() => void pool.end());
  });
}
