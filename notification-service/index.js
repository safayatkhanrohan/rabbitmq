const rabbit = require('../shared/rabbitmq');
const createLogger = require('../shared/logger');
const EVENTS = require('../shared/eventNames');
const { QUEUES } = require('../shared/constants');

const SERVICE = 'notification-service';
const logger = createLogger(SERVICE);

// The notification queue binds to "#", so it receives every event. We only turn
// the terminal outcomes into a customer "email".
const EMAIL_TEMPLATES = {
  [EVENTS.SHIPPING_CREATED]: (p) => `Your order ${p.orderId} has shipped (shipment ${p.shipmentId}).`,
  [EVENTS.PAYMENT_FAILED]: (p) => `Payment failed for order ${p.orderId}: ${p.reason}.`,
  [EVENTS.INVENTORY_FAILED]: (p) => `Order ${p.orderId} could not be fulfilled: ${p.reason}.`,
};

async function start() {
  const { channel } = await rabbit.connect({ logger });
  await rabbit.assertTopology(channel);

  await rabbit.consume(
    channel,
    QUEUES.NOTIFICATION,
    async ({ payload, routingKey, correlationId }) => {
      const template = EMAIL_TEMPLATES[routingKey];
      if (!template) {
        logger.info('event observed (no notification)', { routingKey, correlationId });
        return;
      }
      logger.info('Email sent', { routingKey, correlationId, orderId: payload.orderId, body: template(payload) });
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
