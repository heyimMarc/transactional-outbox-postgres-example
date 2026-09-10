package de.marcnuetzel.outbox.shipment;

import java.time.Instant;
import java.util.UUID;

/** A booked shipment. The business fact that the outbox message talks about. */
public record Shipment(UUID id, String reference, String destination, Instant bookedAt) {
}
