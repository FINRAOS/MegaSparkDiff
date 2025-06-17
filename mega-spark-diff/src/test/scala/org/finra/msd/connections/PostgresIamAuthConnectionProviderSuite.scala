package org.finra.msd.connections

import org.finra.msd.basetestclasses.SparkFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Mock implementation of AuthTokenGenerator for testing purposes.
 * This class simulates token generation without making actual AWS calls,
 * and tracks how many times the token generation method is called.
 */
class MockAuthTokenGenerator extends AuthTokenGenerator {
  // Tracks number of times generateToken is called
  var callCount = 0

  def generateToken(region: String, hostname: String, port: String, username: String): String = {
    callCount += 1
    val token = s"mock-token-$callCount"
    token
  }
}

class PostgresIamAuthConnectionProviderSuite extends SparkFunSuite with Matchers {

  val validOptions: Map[String, String] = Map(
    "url" -> "jdbc:postgresql://test-host:5432/testdb",
    "user" -> "testuser",
    "region" -> "us-east-1",
    "iamAuth" -> "true"
  )

  val driver = new org.postgresql.Driver()

  test("should not handle connections when iamAuth is false") {
    val mockGenerator = new MockAuthTokenGenerator()
    val provider = new PostgresIamAuthConnectionProvider(mockGenerator)
    val optionsWithoutIamAuth = validOptions + ("iamAuth" -> "false")

    provider.canHandle(driver, optionsWithoutIamAuth) shouldBe false
  }

  test("should handle connections when iamAuth is true") {
    val mockGenerator = new MockAuthTokenGenerator()
    val provider = new PostgresIamAuthConnectionProvider(mockGenerator)

    provider.canHandle(driver, validOptions) shouldBe true
  }

  test("should retrieve a token when getting a connection") {
    val mockGenerator = new MockAuthTokenGenerator()
    val provider = new PostgresIamAuthConnectionProvider(mockGenerator)

    intercept[Exception] {
      // This will fail because we can't actually connect to DB,
      // but we can verify token was generated
      provider.getConnection(driver, validOptions)
    }

    mockGenerator.callCount shouldBe 1
  }

  test("should cache token and not regenerate within 11 minutes") {
    val mockGenerator = new MockAuthTokenGenerator()
    val provider = new PostgresIamAuthConnectionProvider(mockGenerator)

    // First call
    intercept[Exception] {
      provider.getConnection(driver, validOptions)
    }

    // Second call immediately after
    intercept[Exception] {
      provider.getConnection(driver, validOptions)
    }

    // Should only have generated token once
    mockGenerator.callCount shouldBe 1
  }

  test("should handle missing required fields") {
    val provider = new PostgresIamAuthConnectionProvider(new MockAuthTokenGenerator())

    // Test missing URL
    val missingUrl = validOptions - "url"
    val urlException = intercept[IllegalArgumentException] {
      provider.getConnection(driver, missingUrl)
    }
    urlException.getMessage should include("url")
    urlException.getMessage should include("required")

    // Test missing user
    val missingUser = validOptions - "user"
    val userException = intercept[IllegalArgumentException] {
      provider.getConnection(driver, missingUser)
    }
    userException.getMessage should include("user")
    userException.getMessage should include("required")

    // Test missing region
    val missingRegion = validOptions - "region"
    val regionException = intercept[IllegalArgumentException] {
      provider.getConnection(driver, missingRegion)
    }
    regionException.getMessage should include("region")
    regionException.getMessage should include("required")
  }

  test("should handle malformed URL") {
    val provider = new PostgresIamAuthConnectionProvider(new MockAuthTokenGenerator())
    val invalidUrlOptions = validOptions + ("url" -> "invalid-url")

    intercept[IllegalArgumentException] {
      provider.getConnection(driver, invalidUrlOptions)
    }.getMessage should include("Failed to parse PostgreSQL JDBC URL")
  }

  test("should parse host and port from URL correctly") {
    val mockGenerator = new MockAuthTokenGenerator() {
      override def generateToken(region: String, hostname: String, port: String, username: String): String = {
        hostname shouldBe "test-host"
        port shouldBe "5432"
        super.generateToken(region, hostname, port, username)
      }
    }

    val provider = new PostgresIamAuthConnectionProvider(mockGenerator)

    intercept[Exception] {
      provider.getConnection(driver, validOptions)
    }
  }

  // Test URL parsing edge cases
  test("should handle URLs with additional parameters") {
    val mockGenerator = new MockAuthTokenGenerator()
    val provider = new PostgresIamAuthConnectionProvider(mockGenerator)
    val optionsWithComplexUrl = validOptions +
      ("url" -> "jdbc:postgresql://test-host:5432/testdb?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory")

    intercept[Exception] {
      provider.getConnection(driver, optionsWithComplexUrl)
    }

    mockGenerator.callCount shouldBe 1
  }
}