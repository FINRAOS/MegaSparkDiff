package org.finra.msd.examples.db;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class H2Database {
  private static String url = "jdbc:h2:./sample";
  private static Properties properties = new Properties();
  static {
    properties.setProperty("user", "username");
    properties.setProperty("password", "password");
  }

  public static void setH2Driver() throws ClassNotFoundException {
    Class.forName("org.h2.Driver");
  }

  public static String getUrl() {
    return url;
  }

  public static Properties getProperties() {
    return properties;
  }

  public static void setUp() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource(
            "h2_db.sql"), Charsets.UTF_8));
      }
    }
  }

  public static void tearDown() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("DROP TABLE appliance");
      }
    }
  }
}
