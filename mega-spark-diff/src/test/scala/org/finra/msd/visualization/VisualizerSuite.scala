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

package org.finra.msd.visualization

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.finra.msd.basetestclasses.SparkFunSuite
import org.finra.msd.containers.DiffResult
import org.finra.msd.memorydb.MemoryDbHsql
import org.finra.msd.sparkcompare.SparkCompare
import org.finra.msd.sparkfactory.SparkFactory

class VisualizerSuite extends SparkFunSuite {

  import testImplicits._

  private def generateString(left: Dataset[Row], right: Dataset[Row], key: String, maxRecords: Int) = {
    //Primary Key as Java List
    val primaryKeySeq = Seq(key)
    val html = Visualizer.generateVisualizerTemplate(left, right, primaryKeySeq, maxRecords)
    html
  }

  private def getAppleTablediffResult(testName1: String, testName2: String) = {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from " + testName1 + ")", "table1")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from " + testName2 + ")", "table2")
    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  test("Visualize as Text") {
    val left = Seq(
      ("key11", "key12", "A", "A"),
      ("key21", "key22", "B", "B"),
      ("4", "4", "C", "C"),
      ("5", "5", "D", "D"),
      ("5", "5", "D", "D"),
      ("6", "6", "E", "E")
    ).toDF("key1", "key2", "value1", "value2")

    val right = Seq(
      ("key11", "key12", null, null),
      ("3", "3", "Y", "Y"),
      ("5", "5", "D", "D"),
      ("6", "6", "E", "E"),
      (null, null, "zz", "zz")
    ).toDF("key1", "key2", "value1", "value2")

    val comparisonResult: DiffResult = SparkCompare.compareSchemaDataFrames(left, right)

    val key: Seq[String] = Seq("key1", "key2")
    val joinedResults: DataFrame = comparisonResult.fullOuterJoinDataFrames(key)
    val html = Visualizer.renderHorizontalTable(joinedResults, 100)

    assert(html.contains("class='different'"))
    assert(html.contains("class='same'"))
    assert(html.contains("</html>"))
  }

  test("basicVisualizerTest") {
    val diffResult = getAppleTablediffResult("EnhancedFruit1", "EnhancedFruit2")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FRUIT", 100)
    if (html.isEmpty) fail("html was empty")
  }

  test("emptyLeftDfTest") {
    val diffResult = getAppleTablediffResult("Fruit4", "Fruit1")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FRUIT", 100)
    if (html.isEmpty) fail("html was empty")
  }

  test("emptyRightDfTest") {
    val diffResult = getAppleTablediffResult("Fruit1", "Fruit4")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FRUIT", 100)
    if (html.isEmpty) fail("html was empty")
  }

  test("nullLeftDfTest") {
    val diffResult = getAppleTablediffResult("Fruit1", "Fruit4")
    val html = generateString(null, diffResult.inRightNotInLeft, "FRUIT", 100)
    assert("<h3>Error message: Left dataframe is null</h3>" == html)
  }

  test("nullRightDfTest") {
    val diffResult = getAppleTablediffResult("Fruit1", "Fruit4")
    val html = generateString(diffResult.inLeftNotInRight, null, "FRUIT", 100)
    assert("<h3>Error message: Right dataframe is null</h3>" == html)
  }

  test("emptyKeyTest") {
    val diffResult = getAppleTablediffResult("Fruit1", "Fruit4")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "", 100)
    assert("<h3>Error message: One or more keys is empty or null</h3>" == html)
  }

  test("nullKeyTest") {
    val diffResult = getAppleTablediffResult("Fruit1", "Fruit4")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, null, 100)
    assert("<h3>Error message: One or more keys is empty or null</h3>" == html)
  }

  test("keyCaseTest") {
    val diffResult = getAppleTablediffResult("Fruit1", "Fruit4")
    var flag = true
    var result1 = ""
    var result2 = ""
    try {
      result1 = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "Fruit", 100)
      result2 = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FrUit", 100)
    } catch {
      case ex: Exception =>
        flag = false
    }
    assert(flag)
    assert(result1 == result2)
  }

  test("invalidMaxRecordsTest") {
    val diffResult = getAppleTablediffResult("Fruit1", "Fruit4")
    var flag = true
    try generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FRUIT", -100)
    catch {
      case ex: Exception =>
        flag = false
    }
    assert(flag)
  }
}
