package org.finra.msd.connections

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import java.sql.{Connection, Driver, DriverManager, SQLException}
import java.util.concurrent.TimeUnit
import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.jdbc.JdbcConnectionProvider

import software.amazon.awssdk.services.rds.RdsUtilities
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

class PostgresIamAuthConnectionProvider extends JdbcConnectionProvider with Logging {
  logInfo("PostgresIamAuthConnectionProvider instantiated")

  private val tokenCache: LoadingCache[(String, String, String, String), String] = CacheBuilder.newBuilder()
    .expireAfterWrite(11, TimeUnit.MINUTES)  // Token expires after 11 minutes
    .build(new CacheLoader[(String, String, String, String), String] {
      override def load(key: (String, String, String, String)): String = {
        val (region, hostName, port, username) = key
        logInfo("Generating new IAM authentication token")
        generateAuthToken(region, hostName, port, username)
      }
    })

  override val name: String = "PostgresIamAuthConnectionProvider"

  override def canHandle(driver: Driver, options: Map[String, String]): Boolean = {
    driver.isInstanceOf[org.postgresql.Driver]
  }

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

  override def modifiesSecurityContext(
                                        driver: Driver,
                                        options: Map[String, String]
                                      ) = false

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