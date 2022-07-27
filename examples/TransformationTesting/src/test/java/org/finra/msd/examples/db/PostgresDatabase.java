package org.finra.msd.examples.db;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PostgresDatabase {
  private static EmbeddedPostgres postgres;

  private static Properties properties = new Properties();
  static {
    properties.setProperty("user", "postgres");
    properties.setProperty("password", "postgres");
  }

  public static String getUrl() {
    return postgres.getJdbcUrl(properties.getProperty("user"), "postgres");
  }

  public static Properties getProperties() {
    return properties;
  }

  public static void startPostgres() throws IOException {
    postgres = EmbeddedPostgres.builder().start();
  }

  public static void stopPostgres() throws IOException {
    postgres.close();
  }

  public static void setUp() throws IOException, SQLException {
    try (Connection conn = postgres.getPostgresDatabase().getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource(
            "pg_db.sql"), Charsets.UTF_8));
      }
    }
  }

  public static void tearDown() throws SQLException {
    try (Connection conn = postgres.getPostgresDatabase().getConnection()) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("DROP TABLE appliance, appliance_type");
      }
    }
  }
}
