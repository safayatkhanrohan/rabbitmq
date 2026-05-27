const rabbit = require('../shared/rabbitmq');
const createLogger = require('../shared/logger');
const EVENTS = require('../shared/eventNames');
const { QUEUES } = require('../shared/constants');

const SERVICE = 'inventory-service';
const logger = createLogger(SERVICE);

// In-memory stock. A real service would hit its own datastore; the plan keeps
// inventory state local to this service. SKU "OUT_OF_STOCK" always fails so the
// failure path is easy to exercise.
const stock = new Map();
const DEFAULT_STOCK = 100;

function available(sku) {
  if (sku === 'OUT_OF_STOCK') return 0;
  return stock.has(sku) ? stock.get(sku) : DEFAULT_STOCK;
}

async function start() {
  const { channel } = await rabbit.connect({ logger });
  await rabbit.assertTopology(channel);

  await rabbit.consume(
    channel,
    QUEUES.INVENTORY,
    async ({ payload, correlationId }) => {
      const { orderId, items, amount } = payload;
      logger.info('checking stock', { orderId, correlationId });

      const shortage = items.find((i) => available(i.sku) < i.qty);

      if (shortage) {
        logger.warn('insufficient stock', { orderId, correlationId, sku: shortage.sku });
        rabbit.publish(channel, EVENTS.INVENTORY_FAILED, { orderId, reason: `out of stock: ${shortage.sku}` }, { correlationId });
        return;
      }

      for (const i of items) stock.set(i.sku, available(i.sku) - i.qty);

      logger.info('stock reserved', { orderId, correlationId });
      rabbit.publish(channel, EVENTS.INVENTORY_RESERVED, { orderId, items, amount }, { correlationId });
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
