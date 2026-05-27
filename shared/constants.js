const EVENTS = require('./eventNames');

const QUEUES = {
  INVENTORY: 'inventory-queue',
  PAYMENT: 'payment-queue',
  SHIPPING: 'shipping-queue',
  NOTIFICATION: 'notification-queue',
};

module.exports = {
  // Primary topic exchange every service publishes to.
  EXCHANGE: 'order-events',
  EXCHANGE_TYPE: 'topic',

  // Retry path: a rejected message is dead-lettered here, parked in a per-queue
  // TTL queue, then routed back to its work queue once the delay elapses.
  RETRY_EXCHANGE: 'order-events.retry',
  RETRY_DELAY_MS: Number(process.env.RETRY_DELAY_MS || 5000),

  // Final resting place once retries are exhausted.
  DLX: 'order-events.dlx',
  DLQ: 'order-events.dlq',

  MAX_RETRIES: Number(process.env.MAX_RETRIES || 3),

  QUEUES,

  // queue -> routing keys it binds to on the topic exchange
  BINDINGS: {
    [QUEUES.INVENTORY]: [EVENTS.ORDER_CREATED],
    [QUEUES.PAYMENT]: [EVENTS.INVENTORY_RESERVED],
    [QUEUES.SHIPPING]: [EVENTS.PAYMENT_SUCCESS],
    [QUEUES.NOTIFICATION]: ['#'],
  },

  RABBITMQ_URL: process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672',
  DATABASE_URL: process.env.DATABASE_URL || 'postgres://admin:admin@localhost:5432/orders',
};
