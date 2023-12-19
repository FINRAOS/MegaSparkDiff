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

import org.finra.msd.basetestclasses.{JsonFormatTests, SparkFunSuite}
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory
import org.finra.msd.sparkfactory.SparkFactory.sparkSession

class JsonToJsonSuite extends SparkFunSuite
  with JsonFormatTests {
  override def returnDiff(jsonFile1: String, jsonFile2: String, sameSchema: Boolean): DiffResult = {
    val schema1 = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(this.getClass.getClassLoader.getResource(jsonFile1).getPath).schema

    val schema2 = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(this.getClass.getClassLoader.getResource(jsonFile2).getPath).schema

    parallelizeTablesAndCompare(jsonFile1, jsonFile2, schema1.fieldNames, if (sameSchema) schema1.fieldNames else schema2.fieldNames)
  }

  def parallelizeTablesAndCompare(jsonFile1: String, jsonFile2: String,
                                  fieldNames1: Array[String], fieldNames2: Array[String]): DiffResult = {
    val filePath1 = this.getClass.getClassLoader.getResource(jsonFile1).getPath
    val leftAppleTable = SparkFactory.parallelizeJSONSource(filePath1, "json_left", fieldNames1)
    val filePath2 = this.getClass.getClassLoader.getResource(jsonFile2).getPath
    val rightAppleTable = SparkFactory.parallelizeJSONSource(filePath2, "json_right", fieldNames2)

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesComplexJsonFormat("compare/JsonTestMapList.json", "compare/JsonTestMapList.json")

  testSameDataTypesComplexDiffValueJsonFormat("compare/JsonTestMapListDiffValue.json", "compare/JsonTestMapList.json")

  testMixedDataTypesSimpleJsonFormat("json/JsonTestSimpleMixedType.json", "json/JsonTestSimpleMixedType.json")

  testMixedDataTypesSimpleDiffJsonFormat("json/JsonTestSimpleMixedType.json", "json/JsonTestSimple.json")

  testSameDataTypesSimpleDiffMissingElementJsonFormat("json/JsonTestSimpleMissingElement.json", "json/JsonTestSimple.json")

  testSameDataTypesSimpleDiffExtraNullElementJsonFormat("json/JsonTestSimpleExtraNull.json", "json/JsonTestSimple.json")
}
