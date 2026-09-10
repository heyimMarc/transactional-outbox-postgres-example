package de.marcnuetzel.outbox.shipment;

import de.marcnuetzel.outbox.relay.OutboxRepository;
import de.marcnuetzel.outbox.support.AbstractOutboxTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShipmentServiceTest extends AbstractOutboxTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-21T09:00:00Z"), ZoneOffset.UTC);

    private ShipmentService service() {
        return new ShipmentService(dataSource, new ShipmentRepository(dataSource), new OutboxRepository(), CLOCK);
    }

    @Test
    @DisplayName("booking writes the shipment and its message in one transaction")
    void writesBothRows() throws SQLException {
        Shipment shipment = service().book("SHP-1001", "Rotterdam");

        assertTrue(new ShipmentRepository(dataSource).findById(shipment.id()).isPresent());
        assertEquals(1, countRows("shipments"));
        assertEquals(1, countRows("outbox"));
        assertEquals(1, countUnpublished(), "the relay has not run yet");
    }

    @Test
    @DisplayName("a failed booking leaves neither a shipment nor a message")
    void rollsBackBoth() throws SQLException {
        ShipmentService service = service();
        service.book("SHP-1001", "Rotterdam");

        // The reference is UNIQUE, so this second booking fails on the
        // shipments insert. Without a shared transaction the outbox row could
        // still survive and announce a shipment that does not exist.
        assertThrows(IllegalStateException.class, () -> service.book("SHP-1001", "Antwerp"));

        assertEquals(1, countRows("shipments"));
        assertEquals(1, countRows("outbox"));
    }
}
