import { readFile } from 'node:fs/promises';
import { pool } from './common.js';

try {
  const migration = await readFile(
    new URL('../migrations/001.sql', import.meta.url),
    'utf8',
  );
  await pool.query(migration);
} finally {
  await pool.end();
}
