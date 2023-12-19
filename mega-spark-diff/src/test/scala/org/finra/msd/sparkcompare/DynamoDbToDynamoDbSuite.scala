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

import org.finra.msd.basetestclasses.{JsonFormatTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory
import org.finra.msd.sparkfactory.SparkFactory.sparkSession

class DynamoDbToDynamoDbSuite extends SparkFunSuiteDynamoDb
  with JsonFormatTests {

  override def returnDiff(table1: String, table2: String, sameSchema: Boolean): DiffResult = {
    val schema1 = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(this.getClass.getClassLoader.getResource(table1 + ".json").getPath).schema

    val schema2 = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(this.getClass.getClassLoader.getResource(table2 + ".json").getPath).schema

    parallelizeTablesAndCompare(table1.replace("/", "_"),
      table2.replace("/", "_"),
      schema1.fieldNames,
      if (sameSchema) schema1.fieldNames else schema2.fieldNames)
  }

  def parallelizeTablesAndCompare(table1: String, table2: String,
                                  fieldNames1: Array[String], fieldNames2: Array[String]): DiffResult = {
    val leftAppleTable = SparkFactory.parallelizeDynamoDBSource(table1, table1 + "_left", fieldNames1)
    val rightAppleTable = SparkFactory.parallelizeDynamoDBSource(table2, table2 + "_right", fieldNames2)

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesComplexJsonFormat("compare/JsonTestMapList", "compare/JsonTestMapList")

  testSameDataTypesComplexDiffValueJsonFormat("compare/JsonTestMapListDiffValue", "compare/JsonTestMapList")

  testMixedDataTypesSimpleJsonFormat("json/JsonTestSimpleMixedType", "json/JsonTestSimpleMixedType")

  testMixedDataTypesSimpleDiffJsonFormat("json/JsonTestSimpleMixedType", "json/JsonTestSimple")

  testSameDataTypesSimpleDiffMissingElementJsonFormat("json/JsonTestSimpleMissingElement", "json/JsonTestSimple")

  testSameDataTypesSimpleDiffExtraNullElementJsonFormat("json/JsonTestSimpleExtraNull", "json/JsonTestSimple")
}
