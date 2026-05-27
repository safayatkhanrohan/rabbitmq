const rabbit = require('../shared/rabbitmq');
const createLogger = require('../shared/logger');
const EVENTS = require('../shared/eventNames');
const { QUEUES } = require('../shared/constants');

const SERVICE = 'payment-service';
const logger = createLogger(SERVICE);

// Simulated payment gateway: declines anything above the limit so the
// payment.failed path is deterministic and easy to test.
const DECLINE_ABOVE = Number(process.env.PAYMENT_DECLINE_ABOVE || 1000);

async function start() {
  const { channel } = await rabbit.connect({ logger });
  await rabbit.assertTopology(channel);

  await rabbit.consume(
    channel,
    QUEUES.PAYMENT,
    async ({ payload, correlationId }) => {
      const { orderId, amount } = payload;
      logger.info('processing payment', { orderId, correlationId, amount });

      if (amount > DECLINE_ABOVE) {
        logger.warn('payment declined', { orderId, correlationId, amount, limit: DECLINE_ABOVE });
        rabbit.publish(channel, EVENTS.PAYMENT_FAILED, { orderId, reason: `amount ${amount} exceeds limit ${DECLINE_ABOVE}` }, { correlationId });
        return;
      }

      logger.info('payment captured', { orderId, correlationId, amount });
      rabbit.publish(channel, EVENTS.PAYMENT_SUCCESS, { orderId, amount }, { correlationId });
    },
    { logger }
  );

  process.on('SIGINT', () => process.exit(0));
  process.on('SIGTERM', () => process.exit(0));
}

start().catch((err) => {
  logger.error('failed to start', { error: err.message });
  process.exit(1);
});
