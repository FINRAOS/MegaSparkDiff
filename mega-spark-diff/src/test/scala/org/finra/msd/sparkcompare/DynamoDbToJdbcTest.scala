package org.finra.msd.sparkcompare

import org.finra.msd.basetestclasses.{JsonFormatToJdbcTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.AppleTable
import org.finra.msd.sparkfactory.SparkFactory

import scala.collection.mutable

class DynamoDbToJdbcTest extends SparkFunSuiteDynamoDb
  with JsonFormatToJdbcTests {
  override def returnDiff(table1: String, table2: String) = {
    val dynamoDbMap = new mutable.HashMap[String, String]
    dynamoDbMap.put("dynamodb.customAWSCredentialsProvider", dynamoDbCustomAWSCredentialsProvider)
    dynamoDbMap.put("dynamodb.endpoint", dynamoDbEndpoint)

    val leftAppleTable = SparkFactory.parallelizeDynamoDBSource(table1, table1 + "_left", dynamoDbMap)
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA", "",
      "(select * from " + table2 + ")", table2 + "_right")

    var rightDf = rightAppleTable.dataFrame

    for (colName <- rightDf.columns) {
      rightDf = rightDf.withColumnRenamed(colName, colName.toLowerCase)
    }

    rightDf = rightDf.selectExpr(leftAppleTable.dataFrame.columns
      .filter(x => rightDf.columns.contains(x)):_*)

    val rightAppleTableLowerCase = AppleTable(rightAppleTable.sourceType, rightDf, rightAppleTable.delimiter, rightAppleTable.tempViewName)

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTableLowerCase)
  }

  testSameDataTypesJsonFormatToJdbc("test_table1", "DynamoDbTest1")

  testMixedDataTypesSimpleJsonFormatToJdbc("test_table2", "DynamoDbTest1")

  testSameDataTypesDiffJsonFormatToJdbc("test_table1", "DynamoDbTest2")

  testMixedDataTypesSimpleDiffJsonFormatToJdbc("test_table2", "DynamoDbTest2")

  testMixedDataTypesWithListDiffJsonFormatToJdbc("test_table3", "DynamoDbTest1")

  testMixedDataTypesWithSetDiffJsonFormatToJdbc("test_table4", "DynamoDbTest1")

  testMixedDataTypesWithMapDiffJsonFormatToJdbc("test_table5", "DynamoDbTest1")

  testSameDataTypesExtraNullColumnJsonFormatToJdbc("test_table6", "DynamoDbTest1")
}
