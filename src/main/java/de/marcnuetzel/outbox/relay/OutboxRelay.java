package de.marcnuetzel.outbox.relay;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.List;

/**
 * Moves messages from the outbox table to the broker.
 *
 * <p>Polling is unfashionable next to logical decoding, and for this job it is
 * usually the right trade. It needs no replication slot, no extra component to
 * operate, and it fails in ways an on-call engineer can reason about at three
 * in the morning. A slot that stops being consumed, by contrast, quietly holds
 * WAL until the disk fills.
 *
 * <p>Delivery is at-least-once. A publish can succeed and the process can die
 * before the row is marked, in which case the message goes out twice. That is
 * the price of not running a distributed transaction, and it is why
 * {@link OutboxMessage#messageId()} exists.
 */
public final class OutboxRelay {

    private final DataSource dataSource;
    private final OutboxRepository outbox;
    private final MessagePublisher publisher;
    private final Clock clock;
    private final int batchSize;

    public OutboxRelay(DataSource dataSource, OutboxRepository outbox, MessagePublisher publisher,
                       Clock clock, int batchSize) {
        this.dataSource = dataSource;
        this.outbox = outbox;
        this.publisher = publisher;
        this.clock = clock;
        this.batchSize = batchSize;
    }

    /**
     * Runs one pass: claim a batch, publish it, mark what went out.
     *
     * <p>Claim, publish and mark share one transaction, so the rows stay locked
     * against other relays for the whole pass. A message whose publish throws
     * is left unpublished with the error recorded, and the next pass picks it
     * up again.
     *
     * @return how many messages were published
     */
    public int relayOnce() {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                List<OutboxMessage> batch = outbox.claimBatch(connection, batchSize);
                int published = 0;
                for (OutboxMessage message : batch) {
                    try {
                        publisher.publish(message);
                        outbox.markPublished(connection, message.id(), clock.instant());
                        published++;
                    } catch (RuntimeException e) {
                        outbox.recordFailure(connection, message.id(), describe(e));
                    }
                }
                connection.commit();
                return published;
            } catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw new IllegalStateException("outbox relay pass failed", e);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("outbox relay pass failed", e);
        }
    }

    private static String describe(RuntimeException e) {
        String message = e.getMessage();
        String text = e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
        return text.length() > 500 ? text.substring(0, 500) : text;
    }
}
