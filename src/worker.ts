import { Queue, Worker, UnrecoverableError } from 'bullmq';
import { pool, connection, transaction, required } from './common.js';

const userAgent = required('DISCOGS_USER_AGENT');
const destination = process.env.DESTINATION_URL;

if (
  destination &&
  !['http:', 'https:'].includes(new URL(destination).protocol)
) {
  throw new Error('Invalid destination URL');
}

const queues = {
  fetch: new Queue('discogs-fetch', { connection }),
  delivery: new Queue('discogs-delivery', { connection }),
};

const workers = (['fetch', 'delivery'] as const).map(
  (stage) => new Worker(
    `discogs-${stage}`,
    async (job) => {
      const event = (
        await pool.query('SELECT * FROM events WHERE id=$1', [
          job.data.eventId,
        ])
      ).rows[0];

      if (!event || event[`${stage}_status`] === 'completed') return;

      if (stage === 'delivery' && !destination) {
        await pool.query(
          `UPDATE events
           SET delivery_status='awaiting_configuration', updated_at=now()
           WHERE id=$1`,
          [event.id],
        );
        return;
      }

      await pool.query(
        `UPDATE events
         SET ${stage}_status='processing',
             ${stage}_attempts=${stage}_attempts+1,
             updated_at=now()
         WHERE id=$1`,
        [event.id],
      );

      try {
        const headers: Record<string, string> = stage === 'fetch'
          ? { 'User-Agent': userAgent }
          : {
              'Content-Type': 'application/json',
              'Idempotency-Key': event.id,
            };
        const token = stage === 'fetch'
          ? process.env.DISCOGS_TOKEN
          : process.env.DESTINATION_TOKEN;

        if (token) {
          headers.Authorization = stage === 'fetch'
            ? `Discogs token=${token}`
            : `Bearer ${token}`;
        }

        const response = await fetch(
          stage === 'fetch'
            ? `https://api.discogs.com/releases/${event.album_id}`
            : destination!,
          {
            method: stage === 'fetch' ? 'GET' : 'POST',
            headers,
            body: stage === 'fetch'
              ? undefined
              : JSON.stringify({
                  eventId: event.id,
                  albumId: Number(event.album_id),
                  data: event.data,
                }),
            signal: AbortSignal.timeout(15000),
            redirect: 'error',
          },
        );

        if (!response.ok) {
          const message = `${stage} HTTP ${response.status}`;
          if (response.status !== 429 && response.status < 500) {
            throw new UnrecoverableError(message);
          }

          const retry = response.headers.get('retry-after');
          const seconds = retry
            ? Number.isFinite(Number(retry))
              ? Number(retry)
              : Math.max(0, (Date.parse(retry) - Date.now()) / 1000)
            : 0;
          const error = new Error(message) as Error & {
            retryAfter?: number;
          };
          error.retryAfter = seconds * 1000;
          throw error;
        }

        const data = stage === 'fetch' ? await response.json() : null;

        await transaction(async (client) => {
          if (stage === 'fetch') {
            await client.query(
              `UPDATE events
               SET data=$2, fetch_status='completed', delivery_status=$3,
                   last_error=NULL, updated_at=now()
               WHERE id=$1`,
              [
                event.id,
                data,
                destination ? 'pending' : 'awaiting_configuration',
              ],
            );
            if (destination) {
              await client.query(
                `INSERT INTO outbox(event_id, stage)
                 VALUES($1, 'delivery') ON CONFLICT DO NOTHING`,
                [event.id],
              );
            }
          } else {
            await client.query(
              `UPDATE events
               SET delivery_status='completed', last_error=NULL,
                   updated_at=now()
               WHERE id=$1`,
              [event.id],
            );
          }
        });
      } catch (error) {
        const failure = error as Error;
        await pool.query(
          `UPDATE events
           SET ${stage}_status=$2, last_error=$3, updated_at=now()
           WHERE id=$1`,
          [
            event.id,
            failure instanceof UnrecoverableError || job.attemptsMade >= 4
              ? 'failed'
              : 'retrying',
            failure.message,
          ],
        );
        throw failure;
      }
    },
    {
      connection,
      concurrency: 1,
      ...(stage === 'fetch'
        ? {
            limiter: {
              max: 1,
              duration: Number(process.env.DISCOGS_INTERVAL_MS || 3000),
            },
          }
        : {}),
      settings: {
        backoffStrategy: (attempts, _type, error) => Math.max(
          2000 * 2 ** (attempts - 1),
          (error as Error & { retryAfter?: number }).retryAfter || 0,
        ),
      },
    },
  ),
);

for (const worker of workers) {
  worker.on('error', (error) => console.error({ error: error.message }));
  worker.on('failed', (job, error) => {
    console.error({ jobId: job?.id, error: error.message });
  });
}

let stopping = false;

async function dispatch() {
  while (!stopping) {
    try {
      await transaction(async (client) => {
        if (destination) {
          const waiting = await client.query(
            `UPDATE events SET delivery_status='pending'
             WHERE delivery_status='awaiting_configuration'
             RETURNING id`,
          );
          for (const row of waiting.rows) {
            await client.query(
              `INSERT INTO outbox(event_id, stage)
               VALUES($1, 'delivery') ON CONFLICT DO NOTHING`,
              [row.id],
            );
          }
        }

        const rows = await client.query(
          `SELECT * FROM outbox ORDER BY id LIMIT 100
           FOR UPDATE SKIP LOCKED`,
        );
        for (const row of rows.rows) {
          await queues[row.stage as keyof typeof queues].add(
            row.stage,
            { eventId: row.event_id },
            {
              jobId: `${row.event_id}-${row.stage}`,
              attempts: 5,
              backoff: { type: 'custom' },
              removeOnComplete: false,
              removeOnFail: false,
            },
          );
          await client.query('DELETE FROM outbox WHERE id=$1', [row.id]);
        }
      });
    } catch (error) {
      console.error({ error: (error as Error).message });
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

const running = dispatch();

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    stopping = true;
    void (async () => {
      await running;
      await Promise.all(workers.map((worker) => worker.close()));
      await Promise.all(Object.values(queues).map((queue) => queue.close()));
      await pool.end();
    })();
  });
}
