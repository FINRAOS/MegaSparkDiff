/*
 * Copyright 2017 MegaSparkDiff Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finra.msd.sparkcompare

import org.finra.msd.basetestclasses.{JsonFormatToJdbcTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.{AppleTable, DiffResult}
import org.finra.msd.memorydb.MemoryDbHsql
import org.finra.msd.sparkfactory.SparkFactory
import org.finra.msd.sparkfactory.SparkFactory.sparkSession

class JsonToJdbcSuite extends SparkFunSuiteDynamoDb
  with JsonFormatToJdbcTests {
  override def returnDiff(jsonFile: String, table: String, sameSchema: Boolean): DiffResult = {
    val schema = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(this.getClass.getClassLoader.getResource(jsonFile).getPath).schema

    parallelizeTablesAndCompare(jsonFile, table.replace("/", "_"), schema.fieldNames)
  }

  def parallelizeTablesAndCompare(jsonFile: String, table: String,
                                  fieldNames: Array[String]): DiffResult = {
    val filePath = this.getClass.getClassLoader.getResource(jsonFile).getPath
    val leftAppleTable = SparkFactory.parallelizeJSONSource(filePath, "json_left", fieldNames)
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from " + table + ")", table + "_right")

    var rightDf = rightAppleTable.dataFrame

    for (colName <- rightDf.columns) {
      rightDf = rightDf.withColumnRenamed(colName, colName.toLowerCase)
    }

    rightDf = rightDf.selectExpr(leftAppleTable.dataFrame.columns
      .filter(x => rightDf.columns.contains(x)): _*)

    val rightAppleTableLowerCase = AppleTable(rightAppleTable.sourceType, rightDf, rightAppleTable.delimiter, rightAppleTable.tempViewName)

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTableLowerCase)
  }

  testSameDataTypesComplexJsonFormat("compare/JsonTestMapList.json", "compare/JsonTestMapList")

  testSameDataTypesComplexDiffValueJsonFormat("compare/JsonTestMapListDiffValue.json", "compare/JsonTestMapList")

  testMixedDataTypesSimpleJsonFormat("json/JsonTestSimpleMixedType.json", "json/JsonTestSimpleMixedType")

  testMixedDataTypesSimpleDiffJsonFormat("json/JsonTestSimpleMixedType.json", "json/JsonTestSimple")

  testSameDataTypesSimpleDiffMissingElementJsonFormat("json/JsonTestSimpleMissingElement.json", "json/JsonTestSimple")

  testSameDataTypesSimpleDiffExtraNullElementJsonFormat("json/JsonTestSimpleExtraNull.json", "json/JsonTestSimple")
}
