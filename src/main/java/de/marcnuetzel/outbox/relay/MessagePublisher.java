package de.marcnuetzel.outbox.relay;

/**
 * Whatever actually talks to the broker: Kafka, Azure Service Bus, an HTTP
 * endpoint. Deliberately an interface here so the example stays runnable
 * without any broker at all, and so the interesting failure case (publishing
 * throws) is easy to provoke in a test.
 */
@FunctionalInterface
public interface MessagePublisher {

    /**
     * Publishes the message. Throwing means the relay leaves the row
     * unpublished and tries again on the next pass.
     */
    void publish(OutboxMessage message);
}
