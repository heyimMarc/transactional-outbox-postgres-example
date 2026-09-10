# Transactional Outbox on PostgreSQL

[![build](https://github.com/heyimMarc/transactional-outbox-postgres-example/actions/workflows/build.yml/badge.svg)](https://github.com/heyimMarc/transactional-outbox-postgres-example/actions/workflows/build.yml)

A small, working example of the transactional outbox pattern. Two tables, one
transaction, plain JDBC. No broker is needed to run it, and none is needed to
understand it.

## The problem

A service books a shipment and has to tell the rest of the system about it.
The obvious code writes the row, commits, then publishes to the broker:

```java
shipments.insert(shipment);   // committed
broker.publish(shipmentBooked); // and if the process dies here?
```

Two systems, one hope. If the process dies between the commit and the publish,
the shipment exists and nobody downstream ever hears about it. Swap the order
and you get the mirror image: a message about a shipment that was rolled back.
Neither ordering fixes it, because the failure is not in the ordering. It is
that two systems are being changed without a shared transaction.

The outbox pattern removes the second system from the critical path. The
message is written into the same database, in the same transaction as the
business row:

```sql
BEGIN;
INSERT INTO shipments (...) VALUES (...);
INSERT INTO outbox (message_id, aggregate_id, message_type, payload, created_at) VALUES (...);
COMMIT;
```

Now both writes succeed or neither does. Getting the message from the table to
the broker becomes a separate, retryable job.

## What's in here

`ShipmentService` books a shipment and writes a `ShipmentBooked` message in one
transaction. `OutboxRelay` polls for unpublished messages and hands them to a
`MessagePublisher`, which is an interface so the example runs without Kafka,
Azure Service Bus, or anything else installed.

The parts worth reading:

**One transaction, two writes.** `OutboxRepository.add` takes a `Connection`
rather than a `DataSource`. That is not a style choice. A repository that
opened its own connection would commit separately and reintroduce the exact
dual write the pattern exists to remove.

**`FOR UPDATE SKIP LOCKED`.** The relay claims a batch with `SKIP LOCKED`, so a
second relay walks past rows the first has locked instead of blocking on them.
Throughput scales with instances and no message is handed to two relays.

**A partial index.** `idx_outbox_unpublished` only covers rows where
`published_at IS NULL`. The relay only ever asks for those, and published rows
drop out of the index instead of growing it forever.

**At-least-once, not exactly-once.** A publish can succeed and the process can
die before the row is marked, and the message goes out twice. That is the price
of not running a distributed transaction. Every message carries a `message_id`
so consumers can deduplicate, and a consumer that ignores it is broken by
design, not by accident.

**Failures stay visible.** A publish that throws leaves the row unpublished
with `attempts` incremented and `last_error` filled in. A stuck message is
diagnosable with a query instead of by grepping relay logs.

## Why polling and not logical decoding

Reading the WAL with Debezium or a replication slot avoids the poll entirely
and is the better answer at high volume. It also adds a component to operate,
and a replication slot that stops being consumed quietly holds WAL until the
disk fills. Polling a table with a partial index costs a cheap indexed query
per interval and fails in ways an on-call engineer can reason about at three in
the morning. Start here, move to decoding when the poll actually hurts.

## Running it

```bash
mvn verify
```

Testcontainers starts a real Postgres, so Docker has to be running. There is no
schema to install by hand and no broker to configure.

The tests are the documentation:

- `ShipmentServiceTest` proves the two writes are atomic, including the case
  where the business insert fails and the message must not survive.
- `OutboxRelayTest` covers a normal pass, a broker outage that leaves the
  message pending with its error recorded, and two relays racing over the same
  table without either double-publishing.

## Layout

```
src/main/java/de/marcnuetzel/outbox/
  schema/SchemaInitializer.java    applies db/schema.sql
  shipment/Shipment.java           the business fact
  shipment/ShipmentRepository.java plain reads and writes
  shipment/ShipmentService.java    the one transaction that matters
  relay/OutboxMessage.java         one row, as handed to a publisher
  relay/OutboxRepository.java      insert, claim with SKIP LOCKED, mark
  relay/MessagePublisher.java      whatever talks to the broker
  relay/OutboxRelay.java           one pass: claim, publish, mark
src/main/resources/db/schema.sql
```

## Stack

Java 25 · Maven · JUnit 5 · Testcontainers (Postgres module) · PostgreSQL JDBC
driver. No Spring, no Hibernate, no JPA, on purpose: the point of this repo is
the transaction boundary, not the framework.

## License

MIT, see [LICENSE](LICENSE).

## Background

A longer write-up lives on my blog: [The Transactional Outbox on PostgreSQL](https://www.marc-nuetzel.de/posts/transactional-outbox-postgresql/) *(publishes 2026-10-26)*.
