// Routing keys used on the topic exchange. Keeping them in one place avoids
// typos that would silently break bindings.
module.exports = {
  ORDER_CREATED: 'order.created',
  INVENTORY_RESERVED: 'inventory.reserved',
  INVENTORY_FAILED: 'inventory.failed',
  PAYMENT_SUCCESS: 'payment.success',
  PAYMENT_FAILED: 'payment.failed',
  SHIPPING_CREATED: 'shipping.created',
};
