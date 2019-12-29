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
 *//*
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
import org.finra.msd.sparkfactory.SparkFactory

class JdbcToFileTest() extends SparkFunSuite {

  private def returnDiff(table1: String, table2: String) = {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA", "",
      "(select * from " + table1 + ")", "table1")
    val file2Path = this.getClass.getClassLoader.getResource(table2 + ".txt").getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(file2Path, "table2")
    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  test("testCompareEqualTables") {
    val diffResult = returnDiff("Test1", "Test2")
    //the expectation is that both tables are equal
    assert(diffResult.inLeftNotInRight.count == 0)
    assert(diffResult.inRightNotInLeft.count == 0)
  }

  test("testCompareJDBCTableToTextFile") {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA", "",
      "(select * from Test4)", "table1")
    val file2Path = this.getClass.getClassLoader.getResource("Test4.txt").getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(file2Path, "table2")
    val diffResult = SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
    //the expectation is that both tables are completely different
    assert(diffResult.inLeftNotInRight.count == 0)
    assert(diffResult.inRightNotInLeft.count == 1)
  }

  test("testCompareAFewDifferences") {
    val diffResult = returnDiff("Test1", "Test3")
    //the expectation is that there are only a few differences
    assert(diffResult.inLeftNotInRight.count == 2)
    assert(diffResult.inRightNotInLeft.count == 2)
  }

  test("testCompareTable1IsSubset") {
    val diffResult = returnDiff("Test4", "Test1")
    //the expectation is that table1 is a complete subset of table2
    assert(diffResult.inLeftNotInRight.count == 0)
    assert(diffResult.inRightNotInLeft.count == 5)
  }

  test("testCompareTable2IsSubset") {
    val diffResult = returnDiff("Test1", "Test5")
    //the expectation is that table2 is a complete subset of table1
    assert(diffResult.inLeftNotInRight.count == 5)
    assert(diffResult.inRightNotInLeft.count == 0)
  }
}