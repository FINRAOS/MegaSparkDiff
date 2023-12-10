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

class DynamoDbToJsonSuite extends SparkFunSuiteDynamoDb
  with JsonFormatTests {

  override def returnDiff(table: String, jsonFile: String, sameSchema: Boolean): DiffResult = {
    val schema1 = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(this.getClass.getClassLoader.getResource(table + ".json").getPath).schema

    val schema2 = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(this.getClass.getClassLoader.getResource(jsonFile).getPath).schema

    parallelizeTablesAndCompare(table.replace("/", "_"), jsonFile,
      schema1.fieldNames,
      if (sameSchema) schema1.fieldNames else schema2.fieldNames)
  }

  def parallelizeTablesAndCompare(table: String, jsonFile: String,
                                  fieldNames1: Array[String], fieldNames2: Array[String]): DiffResult = {
    val leftAppleTable = SparkFactory.parallelizeDynamoDBSource(table, table + "_left", fieldNames1)
    val filePath = this.getClass.getClassLoader.getResource(jsonFile).getPath
    val rightAppleTable = SparkFactory.parallelizeJSONSource(filePath, "json_right", fieldNames2)

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesComplexJsonFormat("compare/JsonTestMapList", "compare/JsonTestMapList.json")

  testSameDataTypesComplexDiffValueJsonFormat("compare/JsonTestMapListDiffValue", "compare/JsonTestMapList.json")

  testMixedDataTypesSimpleJsonFormat("json/JsonTestSimpleMixedType", "json/JsonTestSimpleMixedType.json")

  testMixedDataTypesSimpleDiffJsonFormat("json/JsonTestSimpleMixedType", "json/JsonTestSimple.json")

  testSameDataTypesSimpleDiffMissingElementJsonFormat("json/JsonTestSimpleMissingElement", "json/JsonTestSimple.json")

  testSameDataTypesSimpleDiffExtraNullElementJsonFormat("json/JsonTestSimpleExtraNull", "json/JsonTestSimple.json")
}
