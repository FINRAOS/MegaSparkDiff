package org.finra.msd.sparkcompare

import org.finra.msd.basetestclasses.{JsonFormatTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory

import scala.collection.mutable

class DynamoDbToDynamoDbTest extends SparkFunSuiteDynamoDb
  with JsonFormatTests {

  override def returnDiff(table1: String, table2: String): DiffResult = {
    val dynamoDbMap = new mutable.HashMap[String, String]
    dynamoDbMap.put("dynamodb.customAWSCredentialsProvider", dynamoDbCustomAWSCredentialsProvider)
    dynamoDbMap.put("dynamodb.endpoint", dynamoDbEndpoint)

    val leftAppleTable = SparkFactory.parallelizeDynamoDBSource(table1, table1 + "_left", dynamoDbMap)
    val rightAppleTable = SparkFactory.parallelizeDynamoDBSource(table2, table2 + "_right", dynamoDbMap)

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesJsonFormat("test_table1", "test_table1")

  testMixedDataTypesSimpleJsonFormat("test_table1", "test_table2")

  testSameDataTypesWithListJsonFormat("test_table3", "test_table3")

  testSameDataTypesWithListDiffJsonFormat("test_table3", "test_table3_diff")

  testSameDataTypesWithSetJsonFormat("test_table4", "test_table4")

  testSameDataTypesWithSetDiffJsonFormat("test_table4", "test_table4_diff")

  testSameDataTypesWithMapJsonFormat("test_table5", "test_table5")

  testSameDataTypesWithMapDiffJsonFormat("test_table5", "test_table5_diff")

  testSameDataTypesDiffJsonFormat("test_table1", "test_table1_diff")

  testSameDataTypesMixedColumnsSimpleDiffJsonFormat("test_table2", "test_table2_diff")

  testSameDataTypesExtraNullColumnJsonFormat("test_table1", "test_table6")

  testSameDataTypesExtraNullNestedListColumnJsonFormat("test_table3", "test_table7")

  testSameDataTypesExtraNullNestedMapColumnJsonFormat("test_table5", "test_table8")

  testSameDataTypesWithMapListSetJsonFormat("test_table9", "test_table9")

  testSameDataTypesWithMapListSetDiffJsonFormat("test_table9", "test_table9_diff")
}
