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

import org.finra.msd.basetestclasses.{JsonFormatToFileTests, SparkFunSuiteDynamoDb}
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory
import org.finra.msd.sparkfactory.SparkFactory.sparkSession

class JsonToFileSuite extends SparkFunSuiteDynamoDb
  with JsonFormatToFileTests {
  override def returnDiff(jsonFile: String, textFile: String, sameSchema: Boolean): DiffResult = {
    val schema = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(this.getClass.getClassLoader.getResource(jsonFile).getPath).schema

    parallelizeTablesAndCompare(jsonFile, textFile, schema.fieldNames)
  }

  def parallelizeTablesAndCompare(jsonFile: String, textFile: String,
                                  fieldNames: Array[String]): DiffResult = {
    val filePathJson = this.getClass.getClassLoader.getResource(jsonFile).getPath
    val leftAppleTable = SparkFactory.parallelizeJSONSource(filePathJson, "json_left", fieldNames, Option.apply(";"))
    val filePathText = this.getClass.getClassLoader.getResource(textFile).getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(filePathText, "text_right")

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesComplexJsonFormat("compare/JsonTestMapList.json", "compare/JsonTestMapList.txt")

  testSameDataTypesComplexDiffValueJsonFormat("compare/JsonTestMapListDiffValue.json", "compare/JsonTestMapList.txt")

  testMixedDataTypesSimpleJsonFormat("json/JsonTestSimpleMixedType.json", "json/JsonTestSimpleMixedType.txt")

  testMixedDataTypesSimpleDiffJsonFormat("json/JsonTestSimpleMixedType.json", "json/JsonTestSimple.txt")

  testSameDataTypesSimpleDiffMissingElementJsonFormat("json/JsonTestSimpleMissingElement.json", "json/JsonTestSimple.txt")

  testSameDataTypesSimpleDiffExtraNullElementJsonFormat("json/JsonTestSimpleExtraNull.json", "json/JsonTestSimple.txt")
}
