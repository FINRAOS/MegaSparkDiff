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

import org.finra.msd.basetestclasses.BaseJunitForSparkCompare
import org.finra.msd.sparkfactory.SparkFactory
import org.junit.{Assert, Test}

class JdbcToJdbcTest() extends BaseJunitForSparkCompare {
  private def returnDiff(table1: String, table2: String) = {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb", "SA", "", "(select * from " + table1 + ")", "table1")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb", "SA", "", "(select * from " + table2 + ")", "table2")
    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  @Test def testCompareDifferentSchemas() = {
    var failed = false
    try returnDiff("Persons1", "Test1")
    catch {
      case e: Exception =>
        failed = true
        if (!e.getMessage.contains("Column Names Did Not Match")) Assert.fail("Failed for the wrong reason")
    }
    if (!failed) Assert.fail("Was supposed to fail at schema comparison but didn't")
  }

  @Test def testCompareEqualTables() = {
    val diffResult = returnDiff("Test1", "Test2")
    //the expectation is that both tables are equal
    if (diffResult.inLeftNotInRight.count != 0) Assert.fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
    if (diffResult.inRightNotInLeft.count != 0) Assert.fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
  }

  @Test def testCompareCompletelyDifferent() = {
    val pair = returnDiff("Test4", "Test5")
    //the expectation is that both tables are completely different
    if (pair.inLeftNotInRight.count != 5) Assert.fail("Expected 5 differences coming from left table." + "  Instead, found " + pair.inLeftNotInRight.count + ".")
    if (pair.inRightNotInLeft.count != 5) Assert.fail("Expected 5 differences coming from right table." + "  Instead, found " + pair.inRightNotInLeft.count + ".")
  }

  @Test def testCompareAFewDifferences() = {
    val pair = returnDiff("Test1", "Test3")
    //the expectation is that there are only a few differences
    if (pair.inLeftNotInRight.count != 2) Assert.fail("Expected 2 differences coming from left table." + "  Instead, found " + pair.inLeftNotInRight.count + ".")
    if (pair.inRightNotInLeft.count != 2) Assert.fail("Expected 2 differences coming from right table." + "  Instead, found " + pair.inRightNotInLeft.count + ".")
  }

  @Test def testCompareTable1IsSubset() = {
    val pair = returnDiff("Test4", "Test1")
    //the expectation is that table1 is a complete subset of table2
    if (pair.inLeftNotInRight.count != 0) Assert.fail("Expected 0 differences coming from left table." + "  Instead, found " + pair.inLeftNotInRight.count + ".")
    if (pair.inRightNotInLeft.count != 5) Assert.fail("Expected 5 differences coming from right table." + "  Instead, found " + pair.inRightNotInLeft.count + ".")
  }

  @Test def testCompareTable2IsSubset() = {
    val pair = returnDiff("Test1", "Test5")
    //the expectation is that table2 is a complete subset of table1
    if (pair.inLeftNotInRight.count != 5) Assert.fail("Expected 5 differences coming from left table." + "  Instead, found " + pair.inLeftNotInRight.count + ".")
    if (pair.inRightNotInLeft.count != 0) Assert.fail("Expected 0 differences coming from right table." + "  Instead, found " + pair.inRightNotInLeft.count + ".")
  }
}