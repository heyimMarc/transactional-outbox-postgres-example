package de.marcnuetzel.outbox.relay;

import de.marcnuetzel.outbox.shipment.ShipmentRepository;
import de.marcnuetzel.outbox.shipment.ShipmentService;
import de.marcnuetzel.outbox.support.AbstractOutboxTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboxRelayTest extends AbstractOutboxTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-21T09:00:00Z"), ZoneOffset.UTC);

    private ShipmentService shipmentService() {
        return new ShipmentService(dataSource, new ShipmentRepository(dataSource), new OutboxRepository(), CLOCK);
    }

    private OutboxRelay relayWith(MessagePublisher publisher) {
        return new OutboxRelay(dataSource, new OutboxRepository(), publisher, CLOCK, 10);
    }

    @Test
    @DisplayName("the relay publishes pending messages and does not send them twice")
    void publishesOnce() throws SQLException {
        shipmentService().book("SHP-1001", "Rotterdam");
        shipmentService().book("SHP-1002", "Gdansk");

        List<OutboxMessage> sent = new CopyOnWriteArrayList<>();
        OutboxRelay relay = relayWith(sent::add);

        assertEquals(2, relay.relayOnce());
        assertEquals(0, countUnpublished());

        // A second pass finds nothing left to do.
        assertEquals(0, relay.relayOnce());
        assertEquals(2, sent.size());
        assertTrue(sent.stream().allMatch(m -> m.messageType().equals("ShipmentBooked")));
        assertTrue(sent.stream().allMatch(m -> m.payload().contains("\"destination\"")));
    }

    @Test
    @DisplayName("a broker that rejects a message leaves it pending with the error recorded")
    void failedPublishStaysPending() throws SQLException {
        shipmentService().book("SHP-1001", "Rotterdam");

        OutboxRelay failing = relayWith(message -> {
            throw new IllegalStateException("broker unavailable");
        });

        assertEquals(0, failing.relayOnce());
        assertEquals(1, countUnpublished(), "the message must survive a broker outage");
        assertNotNull(lastErrorOfFirstRow());
        assertTrue(lastErrorOfFirstRow().contains("broker unavailable"));

        // Once the broker is back, the next pass delivers it.
        List<OutboxMessage> sent = new CopyOnWriteArrayList<>();
        assertEquals(1, relayWith(sent::add).relayOnce());
        assertEquals(0, countUnpublished());
        assertEquals(1, sent.size());
    }

    @Test
    @DisplayName("two relays running at once never hand the same message to the broker twice")
    void concurrentRelaysDoNotOverlap() throws Exception {
        for (int i = 0; i < 20; i++) {
            shipmentService().book("SHP-20" + i, "Rotterdam");
        }

        List<OutboxMessage> sent = new CopyOnWriteArrayList<>();
        // Both relays claim from the same table at the same moment. SKIP LOCKED
        // is what keeps them off each other's rows: without it one would block
        // on the other, or worse, both would read the same batch.
        Thread first = new Thread(() -> relayWith(sent::add).relayOnce());
        Thread second = new Thread(() -> relayWith(sent::add).relayOnce());
        first.start();
        second.start();
        first.join();
        second.join();

        assertEquals(0, countUnpublished());
        assertEquals(20, sent.size(), "every message exactly once");
        assertEquals(20, sent.stream().map(OutboxMessage::messageId).distinct().count());
    }
}
