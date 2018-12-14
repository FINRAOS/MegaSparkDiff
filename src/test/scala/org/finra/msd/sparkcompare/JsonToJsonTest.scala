package org.finra.msd.sparkcompare

import org.finra.msd.basetestclasses.{JsonFormatTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory

class JsonToJsonTest extends SparkFunSuiteDynamoDb
  with JsonFormatTests {
  override def returnDiff(jsonFile1: String, jsonFile2: String): DiffResult = {
    val filePath1 = this.getClass.getClassLoader.getResource(jsonFile1).getPath
    val leftAppleTable = SparkFactory.parallelizeJSONSource(filePath1, "json_left")
    val filePath2 = this.getClass.getClassLoader.getResource(jsonFile2).getPath
    val rightAppleTable = SparkFactory.parallelizeJSONSource(filePath2, "json_right")

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesJsonFormat("DynamoDbTest1.json", "DynamoDbTest1.json")

  testMixedDataTypesSimpleJsonFormat("DynamoDbTest1.json", "DynamoDbTest2.json")

  testSameDataTypesWithListJsonFormat("DynamoDbTest3.json", "DynamoDbTest3.json")

  testSameDataTypesWithListDiffJsonFormat("DynamoDbTest3.json", "DynamoDbTest3Diff.json")

  testSameDataTypesWithSetJsonFormat("DynamoDbTest4.json", "DynamoDbTest4.json")

  testSameDataTypesWithSetDiffJsonFormat("DynamoDbTest4.json", "DynamoDbTest4Diff.json")

  testSameDataTypesWithMapJsonFormat("DynamoDbTest5.json", "DynamoDbTest5.json")

  testSameDataTypesWithMapDiffJsonFormat("DynamoDbTest5.json", "DynamoDbTest5Diff.json")

  testSameDataTypesDiffJsonFormat("DynamoDbTest1.json", "DynamoDbTest1Diff.json")

  testSameDataTypesMixedColumnsSimpleDiffJsonFormat("DynamoDbTest2.json", "DynamoDbTest2Diff.json")

  testSameDataTypesExtraNullColumnJsonFormat("DynamoDbTest1.json", "DynamoDbTest6.json")

  testSameDataTypesExtraNullNestedListColumnJsonFormat("DynamoDbTest3.json", "DynamoDbTest7.json")

  testSameDataTypesExtraNullNestedMapColumnJsonFormat("DynamoDbTest5.json", "DynamoDbTest8.json")

  testSameDataTypesWithMapListSetJsonFormat("DynamoDbTest9.json", "DynamoDbTest9.json")

  testSameDataTypesWithMapListSetDiffJsonFormat("DynamoDbTest9.json", "DynamoDbTest9Diff.json")
}
