package org.finra.msd.sparkcompare

import org.apache.hadoop.mapreduce.MRJobConfig
import org.finra.msd.basetestclasses.{JsonFormatTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory

import scala.collection.mutable

class DynamoDbToJsonTest extends SparkFunSuiteDynamoDb
  with JsonFormatTests {

  override def returnDiff(table: String, jsonFile: String): DiffResult = {
    val dynamoDbMap = new mutable.HashMap[String, String]
    dynamoDbMap.put("dynamodb.customAWSCredentialsProvider", dynamoDbCustomAWSCredentialsProvider)
    dynamoDbMap.put("dynamodb.endpoint", dynamoDbEndpoint)
    dynamoDbMap.put("mapreduce.map.memory.mb", String.valueOf(MRJobConfig.DEFAULT_MAP_MEMORY_MB))

    val leftAppleTable = SparkFactory.parallelizeDynamoDBSource(table, table + "_left", dynamoDbMap)
    val filePath = this.getClass.getClassLoader.getResource(jsonFile).getPath
    val rightAppleTable = SparkFactory.parallelizeJSONSource(filePath, "json_right")

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesJsonFormat("test_table1", "DynamoDbTest1.json")

  testMixedDataTypesSimpleJsonFormat("test_table1", "DynamoDbTest2.json")

  testSameDataTypesWithListJsonFormat("test_table3", "DynamoDbTest3.json")

  testSameDataTypesWithListDiffJsonFormat("test_table3", "DynamoDbTest3Diff.json")

  testSameDataTypesWithSetJsonFormat("test_table4", "DynamoDbTest4.json")

  testSameDataTypesWithSetDiffJsonFormat("test_table4", "DynamoDbTest4Diff.json")

  testSameDataTypesWithMapJsonFormat("test_table5", "DynamoDbTest5.json")

  testSameDataTypesWithMapDiffJsonFormat("test_table5", "DynamoDbTest5Diff.json")

  testSameDataTypesDiffJsonFormat("test_table1", "DynamoDbTest1Diff.json")

  testSameDataTypesMixedColumnsSimpleDiffJsonFormat("test_table2", "DynamoDbTest2Diff.json")

  testSameDataTypesExtraNullColumnJsonFormat("test_table1", "DynamoDbTest6.json")

  testSameDataTypesExtraNullNestedListColumnJsonFormat("test_table3", "DynamoDbTest7.json")

  testSameDataTypesExtraNullNestedMapColumnJsonFormat("test_table5", "DynamoDbTest8.json")

  testSameDataTypesWithMapListSetJsonFormat("test_table9", "DynamoDbTest9.json")

  testSameDataTypesWithMapListSetDiffJsonFormat("test_table9", "DynamoDbTest9Diff.json")
}
