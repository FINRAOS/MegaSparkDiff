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

import org.finra.msd.basetestclasses.SparkFunSuite
import org.finra.msd.containers.DiffResult
import org.finra.msd.memorydb.MemoryDbHsql
import org.finra.msd.sparkfactory.SparkFactory

class JdbcToJdbcSuite() extends SparkFunSuite {
  private def returnDiff(table1: String, table2: String): DiffResult = {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from " + table1 + ")", "table1")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from " + table2 + ")", "table2")
    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  private def returnDiffWithSavingResult(table1: String, table2: String, testName: String) : Boolean = {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from " + table1 + ")", "table1")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from " + table2 + ")", "table2")
    val outputPath = outputDirectory + "/"+ testName + "/"

    // The save result will exclude the "Ripeness" column and result order by "PRICE" descending
    SparkCompare.compareAppleTablesSaveResultsWithManipulation(
      leftAppleTable, rightAppleTable, outputPath, singleFileOutput = true, ",",
      Option.apply(Array("Ripeness")), Option.apply(Array("PRICE")), ascOrder = false)
  }

  test("testCompareDifferentSchemas") {
    val reason = "Expected \"Column Names Did Not Match\" exception."
    try {
      returnDiff("Persons1", "Fruit1")
      fail(reason)
    }
    catch {
      case e: Exception =>
        if (!e.getMessage.equals("Column Names Did Not Match")) fail(reason)
      case _: Throwable =>
        fail(reason)
    }
  }

  test("testCompareEqualTables") {
    val expectedDiffs = 0
    val diffResult = returnDiff("Fruit1", "Fruit2")
    //the expectation is that both tables are equal
    helpers.reportDiffs(diffResult, expectedDiffs)
  }

  test("testCompareCompletelyDifferent") {
    val expectedDiffs = 5
    val diffResult = returnDiff("Fruit4", "Fruit5")
    //the expectation is that both tables are completely different
    helpers.reportDiffs(diffResult, expectedDiffs)
  }

  test("testCompareAFewDifferences") {
    val expectedDiffs = 2
    val diffResult = returnDiff("Fruit1", "Fruit3")
    //the expectation is that there are only a few differences
    helpers.reportDiffs(diffResult, expectedDiffs)
  }

  test("testCompareTable1IsSubset") {
    val expectedDiffsLeftNotInRight = 0
    val expectedDiffsRightNotInLeft = 5
    val diffResult = returnDiff("Fruit4", "Fruit1")
    //the expectation is that table1 is a complete subset of table2
    helpers.reportDiffs(diffResult, expectedDiffsLeftNotInRight, expectedDiffsRightNotInLeft)
  }

  test("testCompareTable2IsSubset") {
    val expectedDiffsLeftNotInRight = 5
    val expectedDiffsRightNotInLeft = 0
    val diffResult = returnDiff("Fruit1", "Fruit5")
    //the expectation is that table2 is a complete subset of table1
    helpers.reportDiffs(diffResult, expectedDiffsLeftNotInRight, expectedDiffsRightNotInLeft)
  }

  test("testCompareAndSaveFile") {
    deleteSavedFiles("testCompareAndSaveFile")
    val noDiff = returnDiffWithSavingResult("Fruit1", "Fruit3", "testCompareAndSaveFile")
    if (noDiff)
      fail("Expected differences. Instead found no difference!")
    
    val leftDiff = readSavedFile("testCompareAndSaveFile/inLeftNotInRight").split("\n")
    val rightDiff = readSavedFile("testCompareAndSaveFile/inRightNotInLeft").split("\n")
    if (leftDiff.length != 3) // Includes header line
      fail("Expected 3 rows (1 header and 2 data) coming from left table." + "  Instead, found " + leftDiff.length + ".")
    if (rightDiff.length != 3) // Includes header line
      fail("Expected 3 rows (1 header and 2 data) coming from right table." + "  Instead, found " + rightDiff.length + ".")
      
    val leftCols = leftDiff(0).split(",")
    val rightCols = rightDiff(0).split(",")
    if (leftCols.length != 4)
      fail("Expected 4 columns (3 data columns and 1 repeated row count) returned in left table differences." + "  Instead, found " + leftCols.length + ".")
    if (rightCols.length != 4)
      fail("Expected 4 columns (3 data columns and 1 repeated row count) returned in right table differences." + "  Instead, found " + rightCols.length + ".")
      
    if (leftDiff(0).contains("RIPENESS"))
      fail("Expected ripeness not to be included in left table differences.")
    if (rightDiff(0).contains("Ripeness")) {
      fail("Expected ripeness not to be included in right table differences.")
    }
    if (leftDiff(1).split(",")(1).toInt < leftDiff(2).split(",")(1).toInt)
      fail("Expected results to be sorted descending by price in left table differences.")
    if (rightDiff(1).split(",")(1).toInt < rightDiff(2).split(",")(1).toInt)
      fail("Expected results to be sorted descending by price in right table differences.")
  }

}
