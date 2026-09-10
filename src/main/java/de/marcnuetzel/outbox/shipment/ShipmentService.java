package de.marcnuetzel.outbox.shipment;

import de.marcnuetzel.outbox.relay.OutboxRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/**
 * Books a shipment and announces it.
 *
 * <p>This class is the entire argument for the outbox pattern. The obvious
 * implementation writes the row, commits, then publishes to the broker. That
 * is two systems and one hope: if the process dies between the commit and the
 * publish, the shipment exists and nobody downstream ever hears about it. Move
 * the publish before the commit and you get the mirror image, a message about
 * a shipment that was rolled back.
 *
 * <p>Here both writes go into the same transaction against the same database,
 * so they either both happen or neither does. Getting the message out of the
 * table and onto the broker is a separate, retryable problem, and that is what
 * the relay is for.
 */
public final class ShipmentService {

    private final DataSource dataSource;
    private final ShipmentRepository shipments;
    private final OutboxRepository outbox;
    private final Clock clock;

    public ShipmentService(DataSource dataSource, ShipmentRepository shipments, OutboxRepository outbox, Clock clock) {
        this.dataSource = dataSource;
        this.shipments = shipments;
        this.outbox = outbox;
        this.clock = clock;
    }

    /**
     * Books a shipment and writes the {@code ShipmentBooked} message in one
     * transaction.
     *
     * @throws IllegalStateException if the booking fails, in which case
     *         neither the shipment nor the message exists
     */
    public Shipment book(String reference, String destination) {
        Instant now = clock.instant();
        Shipment shipment = new Shipment(UUID.randomUUID(), reference, destination, now);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                shipments.insert(connection, shipment);
                outbox.add(connection, shipment.id(), "ShipmentBooked", payloadFor(shipment), now);
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw new IllegalStateException("failed to book shipment " + reference, e);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed to book shipment " + reference, e);
        }
        return shipment;
    }

    private static String payloadFor(Shipment shipment) {
        return """
                {"shipmentId":"%s","reference":"%s","destination":"%s","bookedAt":"%s"}"""
                .formatted(shipment.id(), shipment.reference(), shipment.destination(), shipment.bookedAt());
    }
}
