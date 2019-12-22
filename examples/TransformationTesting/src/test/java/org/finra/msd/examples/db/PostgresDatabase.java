package org.finra.msd.examples.db;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import ru.yandex.qatools.embed.postgresql.Command;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.Credentials;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.Net;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.Storage;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.Timeout;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.RuntimeConfigBuilder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PostgresDatabase {
  private static PostgresProcess process;
  private static String url = null;
  private static Properties properties = new Properties();

  public static String getUrl() {
    return url;
  }

  public static Properties getProperties() {
    return properties;
  }

  public static void startPostgres() throws IOException, ClassNotFoundException {
    Class.forName("org.postgresql.Driver");

    IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
        .defaults(Command.Postgres)
        .build();

    PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getInstance(runtimeConfig);

    PostgresConfig config = new PostgresConfig(
        () -> "10.11-1",
        new Net(),
        new Storage("src/test/resources/sample"),
        new Timeout(),
        new Credentials("username", "password"));

    PostgresExecutable exec = runtime.prepare(config);
    process = exec.start();

    properties.setProperty("user", config.credentials().username());
    properties.setProperty("password", config.credentials().password());

    url = java.lang.String.format("jdbc:postgresql://%s:%s/%s",
        config.net().host(),
        config.net().port(),
        config.storage().dbName());
  }

  public static void stopPostgres() throws IOException {
    process.stop();
  }

  public static void setUp() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource(
            "pg_db.sql"), Charsets.UTF_8));
      }
    }
  }

  public static void tearDown() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("DROP TABLE appliance, appliance_type");
      }
    }
  }
}
