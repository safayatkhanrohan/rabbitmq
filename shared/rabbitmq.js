const amqp = require('amqplib');
const {
  EXCHANGE,
  EXCHANGE_TYPE,
  RETRY_EXCHANGE,
  RETRY_DELAY_MS,
  DLX,
  DLQ,
  MAX_RETRIES,
  BINDINGS,
  RABBITMQ_URL,
} = require('./constants');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function redactedUrl() {
  try {
    const u = new URL(RABBITMQ_URL);
    return `${u.protocol}//${u.host}`;
  } catch {
    return 'amqp://(unparseable)';
  }
}

// Connect with exponential backoff so a service started before RabbitMQ is up
// will wait for it rather than crash immediately.
async function connect({ logger }) {
  let attempt = 0;
  for (;;) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      const channel = await connection.createChannel();
      logger.info('connected to RabbitMQ', { url: redactedUrl() });

      connection.on('error', (err) =>
        logger.error('RabbitMQ connection error', { error: err.message })
      );
      connection.on('close', () => {
        logger.error('RabbitMQ connection closed; exiting (restart the service)');
        process.exit(1);
      });

      return { connection, channel };
    } catch (err) {
      attempt += 1;
      const delay = Math.min(30000, 1000 * 2 ** attempt);
      logger.warn('RabbitMQ connect failed; retrying', {
        attempt,
        delayMs: delay,
        error: err.message,
      });
      await sleep(delay);
    }
  }
}

// Declares the full topology. Idempotent, so every service can call it on
// startup and the system works regardless of which one boots first.
async function assertTopology(channel) {
  await channel.assertExchange(EXCHANGE, EXCHANGE_TYPE, { durable: true });
  await channel.assertExchange(RETRY_EXCHANGE, 'direct', { durable: true });
  await channel.assertExchange(DLX, 'topic', { durable: true });

  await channel.assertQueue(DLQ, { durable: true });
  await channel.bindQueue(DLQ, DLX, '#');

  for (const [queue, routingKeys] of Object.entries(BINDINGS)) {
    await channel.assertQueue(queue, {
      durable: true,
      arguments: {
        // A rejected message goes to the retry exchange keyed by queue name.
        'x-dead-letter-exchange': RETRY_EXCHANGE,
        'x-dead-letter-routing-key': queue,
      },
    });
    for (const rk of routingKeys) {
      await channel.bindQueue(queue, EXCHANGE, rk);
    }

    // Parking queue: holds the message for RETRY_DELAY_MS then dead-letters it
    // back to the work queue via the default exchange (routing key = queue name).
    const retryQueue = `${queue}.retry`;
    await channel.assertQueue(retryQueue, {
      durable: true,
      arguments: {
        'x-message-ttl': RETRY_DELAY_MS,
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': queue,
      },
    });
    await channel.bindQueue(retryQueue, RETRY_EXCHANGE, queue);
  }
}

function publish(channel, routingKey, payload, { correlationId } = {}) {
  return channel.publish(EXCHANGE, routingKey, Buffer.from(JSON.stringify(payload)), {
    persistent: true,
    contentType: 'application/json',
    correlationId,
    headers: correlationId ? { correlationId } : {},
  });
}

// How many times this message has already been rejected from its work queue.
function retryCount(msg) {
  const deaths = msg.properties.headers && msg.properties.headers['x-death'];
  if (!Array.isArray(deaths)) return 0;
  return deaths
    .filter((d) => d.reason === 'rejected')
    .reduce((sum, d) => sum + (d.count || 0), 0);
}

function sendToDlq(channel, queue, msg) {
  channel.publish(DLX, queue, msg.content, {
    persistent: true,
    contentType: msg.properties.contentType,
    correlationId: msg.properties.correlationId,
    headers: { ...msg.properties.headers, 'x-dead-letter-from': queue },
  });
}

// Wraps a handler with prefetch, JSON parsing, acks, bounded retries and a
// final dead-letter hop once retries are exhausted.
async function consume(channel, queue, handler, { logger }) {
  await channel.prefetch(1);
  await channel.consume(queue, async (msg) => {
    if (!msg) return;
    const routingKey = msg.fields.routingKey;
    const correlationId = msg.properties.correlationId;

    let payload;
    try {
      payload = JSON.parse(msg.content.toString());
    } catch (err) {
      logger.error('invalid JSON; routing straight to DLQ', { queue, routingKey, correlationId });
      sendToDlq(channel, queue, msg);
      channel.ack(msg);
      return;
    }

    try {
      await handler({ payload, routingKey, correlationId, msg });
      channel.ack(msg);
    } catch (err) {
      const attempts = retryCount(msg);
      if (attempts < MAX_RETRIES) {
        logger.warn('handler failed; scheduling retry', {
          queue,
          routingKey,
          correlationId,
          attempt: attempts + 1,
          maxRetries: MAX_RETRIES,
          error: err.message,
        });
        // requeue=false -> dead-letter to retry exchange -> TTL park -> back here
        channel.nack(msg, false, false);
      } else {
        logger.error('retries exhausted; sending to DLQ', {
          queue,
          routingKey,
          correlationId,
          error: err.message,
        });
        sendToDlq(channel, queue, msg);
        channel.ack(msg);
      }
    }
  });
  logger.info('consuming', { queue });
}

module.exports = { connect, assertTopology, publish, consume };
