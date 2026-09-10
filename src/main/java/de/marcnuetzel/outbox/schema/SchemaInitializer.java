package de.marcnuetzel.outbox.schema;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Applies {@code db/schema.sql}. Real projects reach for Flyway or Liquibase
 * here; this example keeps the dependency list short so the outbox itself
 * stays the only thing worth reading.
 */
public final class SchemaInitializer {

    private SchemaInitializer() {
    }

    public static void initialize(DataSource dataSource) {
        String ddl = readSchema();
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        } catch (SQLException e) {
            throw new IllegalStateException("failed to apply schema", e);
        }
    }

    private static String readSchema() {
        try (InputStream in = SchemaInitializer.class.getResourceAsStream("/db/schema.sql")) {
            if (in == null) {
                throw new IllegalStateException("db/schema.sql not on the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("failed to read db/schema.sql", e);
        }
    }
}
