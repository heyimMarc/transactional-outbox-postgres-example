package de.marcnuetzel.outbox.support;

import de.marcnuetzel.outbox.schema.SchemaInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Base class for tests that need a real Postgres instance. The container is
 * started once per test class and reused; each test starts from empty
 * {@code shipments}/{@code outbox} tables.
 *
 * <p>A real database rather than a mock is not gold plating here. Everything
 * this example claims lives in the database: the atomicity of the two writes,
 * the partial index, and {@code SKIP LOCKED}. None of that can be observed
 * against an in-memory stand-in.
 */
@Testcontainers
public abstract class AbstractOutboxTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    protected static DataSource dataSource;

    @BeforeAll
    static void setUpSchema() {
        PGSimpleDataSource pgDataSource = new PGSimpleDataSource();
        pgDataSource.setUrl(POSTGRES.getJdbcUrl());
        pgDataSource.setUser(POSTGRES.getUsername());
        pgDataSource.setPassword(POSTGRES.getPassword());
        dataSource = pgDataSource;
        SchemaInitializer.initialize(dataSource);
    }

    @AfterEach
    void truncateTables() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE shipments, outbox");
        }
    }

    protected static long countRows(String table) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    protected static long countUnpublished() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT count(*) FROM outbox WHERE published_at IS NULL")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    protected static String lastErrorOfFirstRow() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT last_error FROM outbox ORDER BY id LIMIT 1");
             ResultSet rs = statement.executeQuery()) {
            return rs.next() ? rs.getString("last_error") : null;
        }
    }
}
