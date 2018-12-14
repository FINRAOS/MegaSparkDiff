package org.finra.msd.sparkcompare

import org.finra.msd.basetestclasses.{JsonFormatToFileTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory

class JsonToFileTest extends SparkFunSuiteDynamoDb
  with JsonFormatToFileTests {
  override def returnDiff(jsonFile: String, textFile: String): DiffResult = {
    val filePathJson = this.getClass.getClassLoader.getResource(jsonFile).getPath
    val leftAppleTable = SparkFactory.parallelizeJSONSource(filePathJson, "json_left")
    val filePathText = this.getClass.getClassLoader.getResource(textFile).getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(filePathText, "text_right")

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
