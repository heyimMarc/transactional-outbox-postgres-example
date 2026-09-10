package de.marcnuetzel.outbox.relay;

import java.util.UUID;

/**
 * One row of the outbox, as handed to a {@link MessagePublisher}.
 *
 * <p>{@code messageId} is the deduplication key. The relay guarantees
 * at-least-once delivery, never exactly-once, so a consumer that cannot
 * tolerate seeing the same message twice has to remember this id.
 */
public record OutboxMessage(long id, UUID messageId, UUID aggregateId, String messageType, String payload) {
}
