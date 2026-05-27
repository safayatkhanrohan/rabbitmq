const crypto = require('crypto');
const rabbit = require('../shared/rabbitmq');
const createLogger = require('../shared/logger');
const EVENTS = require('../shared/eventNames');
const { QUEUES } = require('../shared/constants');

const SERVICE = 'shipping-service';
const logger = createLogger(SERVICE);

async function start() {
  const { channel } = await rabbit.connect({ logger });
  await rabbit.assertTopology(channel);

  await rabbit.consume(
    channel,
    QUEUES.SHIPPING,
    async ({ payload, correlationId }) => {
      const { orderId } = payload;
      const shipmentId = crypto.randomUUID();

      logger.info('shipment created', { orderId, shipmentId, correlationId });
      rabbit.publish(channel, EVENTS.SHIPPING_CREATED, { orderId, shipmentId }, { correlationId });
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
