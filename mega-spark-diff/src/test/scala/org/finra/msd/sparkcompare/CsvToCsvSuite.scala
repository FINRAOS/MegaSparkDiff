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

import org.apache.spark.sql.types.{StringType, StructType}
import org.finra.msd.basetestclasses.SparkFunSuite
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkfactory.SparkFactory

class CsvToCsvSuite extends SparkFunSuite {

  private def returnDiff(file1: String, file2: String, delimiter1: String, delimiter2: String): DiffResult = {
    val expectedSchema = new StructType()
      .add("fruit", StringType, nullable = true)
      .add("price", StringType, nullable = true)
      .add("ripeness", StringType, nullable = true)
      .add("color", StringType, nullable = true)

    val file1Path = this.getClass.getClassLoader.getResource(file1).getPath
    val leftAppleTable = SparkFactory.parallelizeCSVSource(file1Path, "left_table", Option(expectedSchema), Option(delimiter1))
    val file2Path = this.getClass.getClassLoader.getResource(file2).getPath
    val rightAppleTable = SparkFactory.parallelizeCSVSource(file2Path, "right_table", Option(expectedSchema), Option(delimiter2))
    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  test("test CSV partial quote to full quote") {
    val expectedDiffs = 0
    val diffResult = returnDiff("csv/TestCSV.txt", "csv/TestCSV_2.txt", "|", ",")
    //comparison complains about column name diffs between the tables (from the schemaStruct/inferSchema)
    helpers.reportDiffs(diffResult, expectedDiffs)
  }

  test("test CSV escaped quotes") {
    val expectedDiffs = 2
    val diffResult = returnDiff("csv/TestCSV_commas.txt", "csv/TestCSV_pipes.txt", ",", "|")
    //comparison complains about column name diffs between the tables (from the schemaStruct/inferSchema)
    helpers.reportDiffs(diffResult, expectedDiffs)
  }
}
