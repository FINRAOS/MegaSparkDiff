package org.finra.msd.sparkcompare

import org.finra.msd.basetestclasses.{JsonFormatToFileTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory

import scala.collection.mutable

class DynamoDbToFileTest extends SparkFunSuiteDynamoDb
  with JsonFormatToFileTests {
  override def returnDiff(table: String, textFile: String): DiffResult = {
    val dynamoDbMap = new mutable.HashMap[String, String]
    dynamoDbMap.put("dynamodb.customAWSCredentialsProvider", dynamoDbCustomAWSCredentialsProvider)
    dynamoDbMap.put("dynamodb.endpoint", dynamoDbEndpoint)

    val leftAppleTable = SparkFactory.parallelizeDynamoDBSource(table, table + "_left", dynamoDbMap)
    val filePath = this.getClass.getClassLoader.getResource(textFile).getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(filePath, "text_right")

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesJsonFormatToFile("test_table1", "DynamoDBTest1.txt")

  testMixedDataTypesSimpleJsonFormatToFile("test_table2", "DynamoDBTest1.txt")

  testSameDataTypesDiffJsonFormatToFile("test_table1", "DynamoDBTest2.txt")

  testMixedDataTypesSimpleDiffJsonFormatToFile("test_table2", "DynamoDBTest2.txt")

  testMixedDataTypesWithListDiffJsonFormatToFile("test_table3", "DynamoDBTest1.txt")

  testMixedDataTypesWithSetDiffJsonFormatToFile("test_table4", "DynamoDBTest1.txt")

  testMixedDataTypesWithMapDiffJsonFormatToFile("test_table5", "DynamoDBTest1.txt")

  testSameDataTypesExtraNullColumnJsonFormatToFile("test_table6", "DynamoDBTest1.txt")
}
