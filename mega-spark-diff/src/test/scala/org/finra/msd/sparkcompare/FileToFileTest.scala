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
import org.junit.{Assert, Test}

class FileToFileTest() extends SparkFunSuite {
  private def returnDiff(fileName1: String, fileName2: String) = {
    val file1Path = this.getClass.getClassLoader.getResource(fileName1 + ".txt").getPath
    val leftAppleTable = SparkFactory.parallelizeTextSource(file1Path, "table1")
    val file2Path = this.getClass.getClassLoader.getResource(fileName2 + ".txt").getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(file2Path, "table2")
    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  test("testCompareEqualFiles") {
    val diffResult = returnDiff("Test1", "Test2")
    //the expectation is that both tables are equal
    if (diffResult.inLeftNotInRight.count != 0)
      fail("Expected 0 differences coming from left file. " +
        "Instead, found " + diffResult.inLeftNotInRight.count + ".")
    if (diffResult.inRightNotInLeft.count != 0)
      fail("Expected 0 differences coming from right file. " +
        "Instead, found " + diffResult.inRightNotInLeft.count + ".")
  }

  test("testCompareCompletelyDifferentFiles") {
    val diffResult = returnDiff("Test4", "Test5")
    //the expectation is that both tables are completely different
    if (diffResult.inLeftNotInRight.count != 5) fail("Expected 5 differences coming from left table. " +
      "Instead, found " + diffResult.inLeftNotInRight.count + ".")
    if (diffResult.inRightNotInLeft.count != 4) fail("Expected 4 differences coming from right table. " +
      "Instead, found " + diffResult.inRightNotInLeft.count + ".")
  }

  test("testCompareAFewDifferences") {
    val diffResult = returnDiff("Test1", "Test3")
    //the expectation is that there are only a few differences
    if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table. " +
      "Instead, found " + diffResult.inLeftNotInRight.count + ".")
    if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table. " +
      "Instead, found " + diffResult.inRightNotInLeft.count + ".")
  }

  test("testCompareTable1IsSubset") {
    val diffResult = returnDiff("Test4", "Test1")
    //the expectation is that table1 is a complete subset of table2
    val leftCount = diffResult.inLeftNotInRight.count
    val rightCount = diffResult.inRightNotInLeft.count
    if (leftCount != 0) fail("Expected 0 differences coming from left table. " +
      "Instead, found " + leftCount + ".")
    if (rightCount != 4) fail("Expected 4 differences coming from right table. " +
      "Instead, found " + rightCount + ".")
  }

  test("testCompareTable2IsSubset") {
    val diffResult = returnDiff("Test1", "Test5")
    //the expectation is that table2 is a complete subset of table1
    if (diffResult.inLeftNotInRight.count != 5)
      fail("Expected 5 differences coming from left table. " +
        "Instead, found " + diffResult.inLeftNotInRight.count + ".")
    if (diffResult.inRightNotInLeft.count != 0)
      fail("Expected 0 differences coming from right table. " +
        "Instead, found " + diffResult.inRightNotInLeft.count + ".")
  }
}