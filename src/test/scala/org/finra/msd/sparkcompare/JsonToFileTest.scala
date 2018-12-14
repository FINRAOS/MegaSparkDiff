package org.finra.msd.sparkcompare

import org.finra.msd.basetestclasses.{JsonFormatToFileTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory

class JsonToFileTest extends SparkFunSuiteDynamoDb
  with JsonFormatToFileTests {
  override def returnDiff(jsonFile: String, textFile: String): DiffResult = {
    val leftAppleTable = SparkFactory.parallelizeJSONSource(System.getProperty("user.dir") + "/target/test-classes/" + jsonFile, "json_left")
    val rightAppleTable = SparkFactory.parallelizeTextSource(System.getProperty("user.dir") + "/target/test-classes/" + textFile, "text_right")

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesJsonFormatToFile("DynamoDbTest1.json", "DynamoDBTest1.txt")

  testMixedDataTypesSimpleJsonFormatToFile("DynamoDbTest2.json", "DynamoDBTest1.txt")

  testSameDataTypesDiffJsonFormatToFile("DynamoDbTest1.json", "DynamoDBTest2.txt")

  testMixedDataTypesSimpleDiffJsonFormatToFile("DynamoDbTest2.json", "DynamoDBTest2.txt")

  testMixedDataTypesWithListDiffJsonFormatToFile("DynamoDbTest3.json", "DynamoDBTest1.txt")

  testMixedDataTypesWithSetDiffJsonFormatToFile("DynamoDbTest4.json", "DynamoDBTest1.txt")

  testMixedDataTypesWithMapDiffJsonFormatToFile("DynamoDbTest5.json", "DynamoDBTest1.txt")

  testSameDataTypesExtraNullColumnJsonFormatToFile("DynamoDbTest6.json", "DynamoDBTest1.txt")
}
