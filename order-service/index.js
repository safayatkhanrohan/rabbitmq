const crypto = require('crypto');
const express = require('express');
const pool = require('../shared/db');
const rabbit = require('../shared/rabbitmq');
const createLogger = require('../shared/logger');
const EVENTS = require('../shared/eventNames');

const SERVICE = 'order-service';
const PORT = Number(process.env.ORDER_PORT || 3000);
const logger = createLogger(SERVICE);

let channel;

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      id             UUID PRIMARY KEY,
      status         TEXT NOT NULL,
      items          JSONB NOT NULL,
      amount         NUMERIC(12,2) NOT NULL,
      correlation_id UUID NOT NULL,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
  logger.info('orders table ready');
}

// items: [{ sku, qty, price }]
function validateOrder(body) {
  if (!body || !Array.isArray(body.items) || body.items.length === 0) {
    return 'items must be a non-empty array';
  }
  for (const item of body.items) {
    if (!item || typeof item.sku !== 'string' || !item.sku) return 'each item needs a sku';
    if (!Number.isInteger(item.qty) || item.qty <= 0) return 'each item needs a positive integer qty';
    if (typeof item.price !== 'number' || item.price < 0) return 'each item needs a non-negative price';
  }
  return null;
}

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => res.json({ status: 'ok', service: SERVICE }));

app.post('/orders', async (req, res) => {
  const validationError = validateOrder(req.body);
  if (validationError) return res.status(400).json({ error: validationError });

  const id = crypto.randomUUID();
  const correlationId = crypto.randomUUID();
  const items = req.body.items;
  const amount = items.reduce((sum, i) => sum + i.qty * i.price, 0);

  try {
    await pool.query(
      `INSERT INTO orders (id, status, items, amount, correlation_id)
       VALUES ($1, $2, $3, $4, $5)`,
      [id, 'created', JSON.stringify(items), amount, correlationId]
    );

    rabbit.publish(channel, EVENTS.ORDER_CREATED, { orderId: id, items, amount }, { correlationId });

    logger.info('order created and published', { orderId: id, correlationId, amount });
    res.status(201).json({ orderId: id, status: 'created', amount, correlationId });
  } catch (err) {
    logger.error('failed to create order', { error: err.message });
    res.status(500).json({ error: 'failed to create order' });
  }
});

app.get('/orders/:id', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM orders WHERE id = $1', [req.params.id]);
    if (rows.length === 0) return res.status(404).json({ error: 'order not found' });
    res.json(rows[0]);
  } catch (err) {
    logger.error('failed to fetch order', { error: err.message });
    res.status(500).json({ error: 'failed to fetch order' });
  }
});

async function start() {
  await initDb();
  const conn = await rabbit.connect({ logger });
  channel = conn.channel;
  await rabbit.assertTopology(channel);

  const server = app.listen(PORT, () => logger.info('listening', { port: PORT }));

  const shutdown = async () => {
    logger.info('shutting down');
    server.close();
    try { await conn.connection.close(); } catch {}
    try { await pool.end(); } catch {}
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

start().catch((err) => {
  logger.error('failed to start', { error: err.message });
  process.exit(1);
});
