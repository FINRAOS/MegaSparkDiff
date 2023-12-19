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

class DynamoDbToFileSuite extends SparkFunSuiteDynamoDb
  with JsonFormatToFileTests {
  override def returnDiff(table: String, textFile: String, sameSchema: Boolean): DiffResult = {
    val schema = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(this.getClass.getClassLoader.getResource(table + ".json").getPath).schema

    parallelizeTablesAndCompare(table.replace("/", "_"), textFile, schema.fieldNames)
  }

  def parallelizeTablesAndCompare(table: String, textFile: String,
                                  fieldNames: Array[String]): DiffResult = {
    val leftAppleTable = SparkFactory.parallelizeDynamoDBSource(table, table + "_left", fieldNames, Option.apply(";"))
    val filePath = this.getClass.getClassLoader.getResource(textFile).getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(filePath, "text_right")

    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  testSameDataTypesComplexJsonFormat("compare/JsonTestMapList", "compare/JsonTestMapList.txt")

  testSameDataTypesComplexDiffValueJsonFormat("compare/JsonTestMapListDiffValue", "compare/JsonTestMapList.txt")

  testMixedDataTypesSimpleJsonFormat("json/JsonTestSimpleMixedType", "json/JsonTestSimpleMixedType.txt")

  testMixedDataTypesSimpleDiffJsonFormat("json/JsonTestSimpleMixedType", "json/JsonTestSimple.txt")

  testSameDataTypesSimpleDiffMissingElementJsonFormat("json/JsonTestSimpleMissingElement", "json/JsonTestSimple.txt")

  testSameDataTypesSimpleDiffExtraNullElementJsonFormat("json/JsonTestSimpleExtraNull", "json/JsonTestSimple.txt")
}
