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

class JdbcToFileSuite() extends SparkFunSuite {

  private def returnDiff(table1: String, table2: String): DiffResult = {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from " + table1 + ")", "table1")
    val file2Path = this.getClass.getClassLoader.getResource(table2 + ".txt").getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(file2Path, "table2")
    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  test("testCompareEqualTables") {
    val expectedDiffs = 0
    val diffResult = returnDiff("Fruit1", "txt/Fruit1")
    //the expectation is that both tables are equal
    helpers.reportDiffs(diffResult, expectedDiffs)
  }

  test("testCompareJDBCTableToTextFile") {
    val expectedDiffsLeftNotInRight = 0
    val expectedDiffsRightNotInLeft = 1
    val diffResult = returnDiff("Fruit4", "txt/Fruit4")
    //the expectation is that both tables are completely different
    helpers.reportDiffs(diffResult, expectedDiffsLeftNotInRight, expectedDiffsRightNotInLeft)
  }

  test("testCompareAFewDifferences") {
    val expectedDiffs = 2
    val diffResult = returnDiff("Fruit1", "txt/Fruit3")
    //the expectation is that there are only a few differences
    helpers.reportDiffs(diffResult, expectedDiffs)
  }

  test("testCompareTable1IsSubset") {
    val expectedDiffsLeftNotInRight = 0
    val expectedDiffsRightNotInLeft = 5
    val diffResult = returnDiff("Fruit4", "txt/Fruit1")
    //the expectation is that table1 is a complete subset of table2
    helpers.reportDiffs(diffResult, expectedDiffsLeftNotInRight, expectedDiffsRightNotInLeft)
  }

  test("testCompareTable2IsSubset") {
    val expectedDiffsLeftNotInRight = 5
    val expectedDiffsRightNotInLeft = 0
    val diffResult = returnDiff("Fruit1", "txt/Fruit5")
    //the expectation is that table2 is a complete subset of table1
    helpers.reportDiffs(diffResult, expectedDiffsLeftNotInRight, expectedDiffsRightNotInLeft)
  }
}