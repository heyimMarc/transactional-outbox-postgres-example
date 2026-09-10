package de.marcnuetzel.outbox.relay;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Reads and writes the {@code outbox} table.
 *
 * <p>Every method takes a {@link Connection} instead of a {@code DataSource}.
 * That is the whole point of the pattern: the insert has to run inside the
 * caller's transaction, next to the business write. A repository that opened
 * its own connection would commit separately and reintroduce exactly the dual
 * write this pattern exists to remove.
 */
public final class OutboxRepository {

    /** Appends a message to the outbox, inside the caller's transaction. */
    public void add(Connection connection, UUID aggregateId, String messageType, String payload, Instant now)
            throws SQLException {
        String sql = """
                INSERT INTO outbox (message_id, aggregate_id, message_type, payload, created_at)
                VALUES (?, ?, ?, ?::jsonb, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, UUID.randomUUID());
            statement.setObject(2, aggregateId);
            statement.setString(3, messageType);
            statement.setString(4, payload);
            statement.setTimestamp(5, Timestamp.from(now));
            statement.executeUpdate();
        }
    }

    /**
     * Claims up to {@code batchSize} unpublished messages, oldest first.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} is what makes it safe to run more than
     * one relay. A second relay hitting rows the first has locked walks past
     * them instead of blocking, so throughput scales with instances and no
     * message is handed to two relays at once.
     *
     * <p>The rows stay locked until the caller commits, which is why the relay
     * publishes and marks inside the same transaction.
     */
    public List<OutboxMessage> claimBatch(Connection connection, int batchSize) throws SQLException {
        String sql = """
                SELECT id, message_id, aggregate_id, message_type, payload
                FROM outbox
                WHERE published_at IS NULL
                ORDER BY id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """;
        List<OutboxMessage> messages = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, batchSize);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    messages.add(new OutboxMessage(
                            rs.getLong("id"),
                            rs.getObject("message_id", UUID.class),
                            rs.getObject("aggregate_id", UUID.class),
                            rs.getString("message_type"),
                            rs.getString("payload")));
                }
            }
        }
        return messages;
    }

    /** Marks a message as delivered so the next pass skips it. */
    public void markPublished(Connection connection, long id, Instant now) throws SQLException {
        String sql = "UPDATE outbox SET published_at = ?, attempts = attempts + 1 WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(now));
            statement.setLong(2, id);
            statement.executeUpdate();
        }
    }

    /**
     * Records a failed attempt without marking the row published, so the
     * message is retried. Keeping the error on the row means a stuck message
     * can be diagnosed with a query instead of by grepping relay logs.
     */
    public void recordFailure(Connection connection, long id, String error) throws SQLException {
        String sql = "UPDATE outbox SET attempts = attempts + 1, last_error = ? WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, error);
            statement.setLong(2, id);
            statement.executeUpdate();
        }
    }
}
