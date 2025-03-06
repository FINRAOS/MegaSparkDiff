package org.finra.msd.connections

import java.sql.{Connection, Driver, DriverManager, SQLException}
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.jdbc.JdbcConnectionProvider

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.rds.RdsUtilities
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest


/**
 * A connection provider that enables IAM authentication for PostgreSQL databases.
 * This provider supports both standard password-based authentication and AWS IAM authentication.
 */
class PostgresIamAuthConnectionProvider extends JdbcConnectionProvider with Logging {
  logInfo("PostgresIamAuthConnectionProvider instantiated")

  /**
   * Cache for storing IAM authentication tokens to avoid generating new tokens for every connection.
   * The cache key is a tuple of (region, hostName, port, username).
   *
   * Tokens are automatically expired after 11 minutes (AWS IAM auth tokens typically have a 15-minute lifetime)
   */
  private val tokenCache: LoadingCache[(String, String, String, String), String] = CacheBuilder.newBuilder()
    .expireAfterWrite(11, TimeUnit.MINUTES)  // Token expires after 11 minutes
    .build(new CacheLoader[(String, String, String, String), String] {
      override def load(key: (String, String, String, String)): String = {
        val (region, hostName, port, username) = key
        logInfo("Generating new IAM authentication token")
        generateAuthToken(region, hostName, port, username)
      }
    })

  /**
   * The name of this connection provider.
   *
   * @return The string identifier for this connection provider
   */
  override val name: String = "PostgresIamAuthConnectionProvider"

  /**
   * Determines if this provider can handle the given driver and options.
   *
   * @param driver The JDBC driver to check
   * @param options Connection options map
   * @return true if the driver is a PostgreSQL driver, false otherwise
   */
  override def canHandle(driver: Driver, options: Map[String, String]): Boolean = {
    driver.isInstanceOf[org.postgresql.Driver]
  }

  /**
   * Creates a database connection using either IAM authentication or password authentication.
   *
   * @param driver The JDBC driver to use for the connection
   * @param options A map containing connection options. Required keys:
   *               - url: The JDBC URL for the database
   *               - user: The username for authentication
   *               Optional keys:
   *               - password: Required if iamAuth is false
   *               - region: AWS region (required for IAM authentication)
   *               - iamAuth: Set to "true" to use IAM authentication
   * @return A Connection object to the database
   * @throws IllegalArgumentException if required parameters are missing or invalid
   * @throws RuntimeException if connection creation fails
   */
  override def getConnection(driver: Driver, options: Map[String, String]): Connection = {
    val url = options.getOrElse("url", throw new IllegalArgumentException("URL must be specified"))
    val user = options.getOrElse("user", throw new IllegalArgumentException("User must be specified"))
    val region = options.getOrElse("region", "")
    val password = options.getOrElse("password", "")
    val iamAuth = options.getOrElse("iamAuth", false)

    if (password.isEmpty && !iamAuth.equals("true")) {
      throw new IllegalArgumentException("Password must be specified if iamAuth is false")
    }

    val properties = new Properties()
    properties.setProperty("user", user)

    if (iamAuth.equals("true")) {
      val (host, port) = getHostAndPortFromUrl(url)
      val authToken = tokenCache.get(region, host, port, user)
      properties.setProperty("password",authToken)
    } else {
      properties.setProperty("password", password)
    }
    try {
      DriverManager.getConnection(url, properties);
    } catch {
      case e: SQLException =>
        throw new RuntimeException(s"Failed to create PostgreSQL connection: ${e.getMessage}", e)
    }
  }

  /**
   * Indicates whether this provider modifies the security context.
   *
   * @param driver The JDBC driver
   * @param options Connection options map
   * @return false as this provider does not modify the security context
   */
  override def modifiesSecurityContext(
                                        driver: Driver,
                                        options: Map[String, String]
                                      ) = false

  /**
   * Generates an AWS IAM authentication token for RDS PostgreSQL.
   *
   * @param region AWS region where the RDS instance is located
   * @param hostName The hostname of the RDS instance
   * @param port The port number of the RDS instance
   * @param username The database username
   * @return An authentication token for RDS IAM authentication
   */
  private def generateAuthToken(region: String, hostName: String, port: String, username: String): String = {

    val rdsUtilities = RdsUtilities.builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .region(Region.of(region))
      .build()

    val authToken = rdsUtilities.generateAuthenticationToken(
      GenerateAuthenticationTokenRequest.builder()
        .hostname(hostName)
        .port(port.toInt)
        .username(username)
        .build()
    )

    authToken
  }

  /**
   * Extracts the hostname and port from a PostgreSQL JDBC URL.
   *
   * @param url The JDBC URL to parse (must start with "jdbc:postgresql://")
   * @return A tuple of (hostname, port), where port defaults to "5432" if not specified
   * @throws IllegalArgumentException if the URL format is invalid
   */
  private def getHostAndPortFromUrl(url: String): (String, String) = {
    try {
      val postgresPrefix = "jdbc:postgresql://"
      if (!url.startsWith(postgresPrefix)) {
        throw new IllegalArgumentException(s"URL must be a PostgreSQL JDBC URL starting with $postgresPrefix")
      }

      // Extract everything after jdbc:postgresql:// and before any subsequent /
      val hostPort = url.substring(postgresPrefix.length).split("/")(0)

      hostPort.split(":") match {
        case Array(host) =>
          // No port specified, use default PostgreSQL port
          (host, "5432")
        case Array(host, port) =>
          (host, port)
        case _ =>
          throw new IllegalArgumentException(s"Invalid PostgreSQL JDBC URL format: $url")
      }
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Failed to parse PostgreSQL JDBC URL: $url", e)
    }
  }

}