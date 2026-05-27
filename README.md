# Distributed Order System with RabbitMQ

A local, event-driven order processing system built with Node.js, Express, RabbitMQ, and PostgreSQL. Services communicate asynchronously through a message broker. No cloud services. No Docker. All services run natively on your machine.

## Features

- Event-driven architecture: services communicate only through async events, no sync calls
- Durable queues with explicit acknowledgments
- Automatic retries with exponential backoff and TTL-based parking
- Dead-letter queues for failed messages
- Correlation IDs for tracing orders across services
- Structured JSON logging with timestamps and context
- Topic-based routing for flexible message distribution
- Graceful degradation: services wait for RabbitMQ instead of crashing

## Architecture

```
POST /orders
    ↓
┌─────────────┐
│Order Service│ (REST, DB, Producer)
└──────┬──────┘
       │ publish: order.created
       ↓
┌──────────────────────────────────┐
│      RabbitMQ Topic Exchange     │
│       (order-events)             │
└─────┬────────┬─────────┬─────────┘
      │        │         │
      ↓        ↓         ↓
┌──────────┐ ┌─────────┐ ┌────────────┐
│Inventory │ │ Payment │ │Notification│
│ Service  │ │ Service │ │  Service   │
└────┬─────┘ └────┬────┘ └────────────┘
     │            │
     ↓            ↓
    (binds to inventory.reserved)
    (binds to payment.success)
┌────────────┐
│ Shipping   │
│  Service   │
└────────────┘
```

### Services

| Service | Consumes | Publishes | Role |
|---------|----------|-----------|------|
| **Order** | — | `order.created` | REST API, persists orders to PostgreSQL |
| **Inventory** | `order.created` | `inventory.reserved` or `inventory.failed` | Checks stock, reserves items |
| **Payment** | `inventory.reserved` | `payment.success` or `payment.failed` | Simulates payment processing |
| **Shipping** | `payment.success` | `shipping.created` | Creates shipments |
| **Notification** | all events (`#`) | — | Sends "email" on terminal outcomes |

## Setup

### Prerequisites

- Node.js 20+
- PostgreSQL (running)
- Linux/macOS with `sudo` access (for RabbitMQ install)

### 1. Install Infrastructure

```bash
./setup.sh
```

This will:
- Install and start RabbitMQ
- Enable the management plugin (UI at `http://localhost:15672`, guest/guest)
- Create PostgreSQL user `admin` and database `orders`

### 2. Install Dependencies

```bash
npm install
```

### 3. Start Services

Each service runs in its own terminal:

```bash
# Terminal 1: Order Service (REST API)
npm run order

# Terminal 2: Inventory Service
npm run inventory

# Terminal 3: Payment Service
npm run payment

# Terminal 4: Shipping Service
npm run shipping

# Terminal 5: Notification Service
npm run notification
```

All services will log to stdout as JSON for easy parsing and correlation.

## Testing

### Happy Path: Successful Order

```bash
curl -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "items": [
      { "sku": "BOOK", "qty": 2, "price": 15.99 },
      { "sku": "PEN", "qty": 5, "price": 1.50 }
    ]
  }'
```

Watch the services log the flow:
1. Order Service: order created, publishes `order.created`
2. Inventory Service: stock reserved, publishes `inventory.reserved`
3. Payment Service: payment captured, publishes `payment.success`
4. Shipping Service: shipment created, publishes `shipping.created`
5. Notification Service: "Email sent"

### Failure: Payment Declined

Amount > $1,000 is declined:

```bash
curl -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "items": [
      { "sku": "TV", "qty": 1, "price": 1500 }
    ]
  }'
```

Flow stops at payment, `payment.failed` published, notification sends "Email sent" for failure.

### Failure: Out of Stock

Special SKU `OUT_OF_STOCK` always fails inventory:

```bash
curl -X POST http://localhost:3000/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "items": [
      { "sku": "OUT_OF_STOCK", "qty": 1, "price": 100 }
    ]
  }'
```

Inventory publishes `inventory.failed`, order stops, notification sends "Email sent".

### Fetch Order

```bash
curl http://localhost:3000/orders/<orderId>
```

Returns the order record (note: status is always "created" in this learning version; see [Limitations](#limitations)).

## RabbitMQ Topology

### Exchanges

- `order-events` (topic, durable): primary event bus
- `order-events.retry` (direct, durable): dead-letter route for retries
- `order-events.dlx` (topic, durable): final dead-letter exchange

### Queues & Bindings

| Queue | Binding | Purpose |
|-------|---------|---------|
| `inventory-queue` | `order.created` | Inventory service consumes order events |
| `payment-queue` | `inventory.reserved` | Payment service consumes after stock reserved |
| `shipping-queue` | `payment.success` | Shipping service consumes after payment |
| `notification-queue` | `#` (all events) | Notification service sees everything |
| `order-events.dlq` | `#` | Final resting place for failed messages |
| `*-queue.retry` | per-queue | TTL parking lots for retry loops |

### Retry & Dead-Letter Flow

When a handler throws an error:

1. **Attempt 1–3**: Message `nack`s → dead-letters to retry exchange → parks in `<queue>.retry` with 5s TTL → returns to work queue
2. **Attempt 4+**: Message goes to DLQ (`order-events.dlq`) and is acked

Each retry attempt is logged with `attempt`, `maxRetries`, and error details.

## Configuration

Environment variables (all optional; defaults work for local setup):

```bash
# Ports
ORDER_PORT=3000                   # order-service REST port

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672
RETRY_DELAY_MS=5000               # how long to park failed messages
MAX_RETRIES=3                      # retry attempts before DLQ

# PostgreSQL
DATABASE_URL=postgres://admin:admin@localhost:5432/orders

# Payment simulation
PAYMENT_DECLINE_ABOVE=1000         # amounts > this are declined
```

Example:

```bash
PAYMENT_DECLINE_ABOVE=500 npm run payment
```

## Project Structure

```
.
├── README.md                      # This file
├── package.json                   # Root dependencies + scripts
├── setup.sh                       # Infra setup (sudo)
│
├── shared/                        # Shared modules
│   ├── eventNames.js              # Routing keys (order.created, etc.)
│   ├── constants.js               # Exchange/queue/DLX configuration
│   ├── rabbitmq.js                # AMQP client (connect, publish, consume)
│   ├── logger.js                  # Structured JSON logging
│   └── db.js                      # PostgreSQL pool
│
├── order-service/
│   └── index.js                   # REST API + order creation
├── inventory-service/
│   └── index.js                   # Stock checking + reservations
├── payment-service/
│   └── index.js                   # Payment processing
├── shipping-service/
│   └── index.js                   # Shipment creation
└── notification-service/
    └── index.js                   # Event observer + email simulator
```

## Key Design Patterns

### 1. Correlation IDs
Every order gets a UUID `correlationId` at creation time. It propagates through message headers across all services, threading a single order's journey through the logs.

```json
{"ts":"2026-05-27T18:59:29.566Z","level":"info","service":"order-service","message":"order created","orderId":"abc-123","correlationId":"def-456"}
```

Grep by `correlationId` to see an order's full lifecycle.

### 2. Event-Driven, No Sync Calls
Services never call each other's APIs. They only publish events to the exchange and consume from their queues. This keeps them decoupled and independent.

### 3. Durable & Acknowledged
All queues are durable (survive broker restart). Consumers explicitly `ack` messages after processing, so a crash mid-handler won't lose the message—it's requeued for retry.

### 4. Graceful Degradation
If RabbitMQ isn't up when a service starts, it logs a warning and retries with exponential backoff (1s, 2s, 4s, ..., capped at 30s). No crash. Useful for local development when services boot in any order.

## Design Choices

1. **Order Status**: Order status is set to "created" at order creation and does not update as events flow. To implement full status tracking, the order-service would need to consume terminal events (payment.success/failed, shipping.created) to update the order—the **saga pattern**. Omitted here for simplicity.

2. **No Distributed Transactions**: Failures are handled at the service level (retry logic, DLQ). If a service fails partway through processing, the message goes to the DLQ for manual inspection/replay. No automatic rollback.

3. **No Event Log**: Events are consumed and forgotten. Production systems often event-source (append-only log) for audit and recovery.

4. **Stock is In-Memory**: Inventory state is held in-memory and resets on restart. For persistence, use a database.

5. **Payment is Simulated**: The payment service declines orders above a configurable threshold rather than calling a real payment gateway.

## Troubleshooting

### RabbitMQ connection refused
`./setup.sh` not run yet. RabbitMQ isn't installed or not running. Run the setup script and ensure `systemctl status rabbitmq-server` shows active.

### PostgreSQL connection error
`./setup.sh` didn't create the `admin` user or `orders` database. Rerun the setup script or manually:
```bash
sudo -u postgres psql -c "CREATE USER admin WITH PASSWORD 'admin';"
sudo -u postgres psql -c "CREATE DATABASE orders OWNER admin;"
```

### Messages stuck in DLQ
Check the RabbitMQ management UI: `http://localhost:15672` (guest/guest). Navigate to Queues → `order-events.dlq`. Messages here failed all retries. Inspect the message body to see what went wrong.

### Service crashes on startup
Check the logs (JSON, piped to stdout). Most common: RabbitMQ or PostgreSQL not reachable. Ensure both are running:
```bash
systemctl status rabbitmq-server
systemctl status postgresql
```

## Extending This System

- **DLQ Inspection UI**: web dashboard to inspect and replay dead-letter messages
- **Saga Pattern**: have order-service consume events to update order status
- **Event Sourcing**: persist all events to an append-only log for audit and recovery
- **Database Inventory**: replace in-memory stock with a persistent data store
- **Real Payment Gateway**: integrate with Stripe, Square, or another processor
- **Monitoring & Alerting**: Prometheus metrics, PagerDuty alerts on DLQ depth
- **Load Testing**: benchmark throughput and latency under high message volume

## References

- [RabbitMQ Official Documentation](https://www.rabbitmq.com/documentation.html)
- [amqplib (Node AMQP Client)](https://www.npmjs.com/package/amqplib)
- [AMQP 0.9.1 Protocol](https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf)
- [Event-Driven Architecture Patterns](https://martinfowler.com/articles/201701-event-driven.html)

---

Questions or issues? Open an issue or send a PR.
