package de.marcnuetzel.outbox.shipment;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;

/** Plain reads and writes on {@code shipments}. */
public final class ShipmentRepository {

    private final DataSource dataSource;

    public ShipmentRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Inserts inside the caller's transaction, alongside the outbox row. */
    public void insert(Connection connection, Shipment shipment) throws SQLException {
        String sql = """
                INSERT INTO shipments (id, reference, destination, booked_at)
                VALUES (?, ?, ?, ?)
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, shipment.id());
            statement.setString(2, shipment.reference());
            statement.setString(3, shipment.destination());
            statement.setTimestamp(4, Timestamp.from(shipment.bookedAt()));
            statement.executeUpdate();
        }
    }

    public Optional<Shipment> findById(UUID id) {
        String sql = "SELECT id, reference, destination, booked_at FROM shipments WHERE id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, id);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new Shipment(
                        rs.getObject("id", UUID.class),
                        rs.getString("reference"),
                        rs.getString("destination"),
                        rs.getTimestamp("booked_at").toInstant()));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed to load shipment " + id, e);
        }
    }
}
