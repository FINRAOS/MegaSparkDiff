package org.finra.msd.sparkcompare

import org.finra.msd.basetestclasses.{JsonFormatToJdbcTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.{AppleTable, DiffResult}
import org.finra.msd.sparkfactory.SparkFactory

class JsonToJdbcTest extends SparkFunSuiteDynamoDb
  with JsonFormatToJdbcTests {
  override def returnDiff(jsonFile: String, table: String): DiffResult = {
    val leftAppleTable = SparkFactory.parallelizeJSONSource(System.getProperty("user.dir") + "/target/test-classes/" + jsonFile, "json_left")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA", "",
      "(select * from " + table + ")", table + "_right")

    var rightDf = rightAppleTable.dataFrame

    for (colName <- rightDf.columns) {
      rightDf = rightDf.withColumnRenamed(colName, colName.toLowerCase)
    }

    rightDf = rightDf.selectExpr(leftAppleTable.dataFrame.columns
      .filter(x => rightDf.columns.contains(x)):_*)

    val rightAppleTableLowerCase = AppleTable(rightAppleTable.sourceType, rightDf, rightAppleTable.delimiter, rightAppleTable.tempViewName)

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTableLowerCase)
  }

  testSameDataTypesJsonFormatToJdbc("DynamoDbTest1.json", "DynamoDbTest1")

  testMixedDataTypesSimpleJsonFormatToJdbc("DynamoDbTest2.json", "DynamoDbTest1")

  testSameDataTypesDiffJsonFormatToJdbc("DynamoDbTest1.json", "DynamoDbTest2")

  testMixedDataTypesSimpleDiffJsonFormatToJdbc("DynamoDbTest2.json", "DynamoDbTest2")

  testMixedDataTypesWithListDiffJsonFormatToJdbc("DynamoDbTest3.json", "DynamoDbTest1")

  testMixedDataTypesWithSetDiffJsonFormatToJdbc("DynamoDbTest4.json", "DynamoDbTest1")

  testMixedDataTypesWithMapDiffJsonFormatToJdbc("DynamoDbTest5.json", "DynamoDbTest1")

  testSameDataTypesExtraNullColumnJsonFormatToJdbc("DynamoDbTest6.json", "DynamoDbTest1")
}
