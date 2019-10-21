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

class DelimitedVsDelimitedSourceTest() extends SparkFunSuite {
  private def returnDiff(fileName1: String, fileName2: String) = {
    val file1Path = this.getClass.getClassLoader.getResource(fileName1).getPath
    val leftAppleTable = SparkFactory.parallelizeDelimitedSource(file1Path, "table1")
    val file2Path = this.getClass.getClassLoader.getResource(fileName2).getPath
    val rightAppleTable = SparkFactory.parallelizeDelimitedSource(file2Path, "table2")
    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }

  test("testCompareEqualFiles") {
    val diffResult = returnDiff("Test1.txt", "Test2.txt")
    assert(diffResult.inLeftNotInRight.count == 0)
    assert(diffResult.inRightNotInLeft.count == 0)
  }

  test("testCompareCompletelyDifferentFiles") {
    val diffResult = returnDiff("Test4.txt", "Test5.txt")
    assert(diffResult.inLeftNotInRight.count == 5)
    assert(diffResult.inRightNotInLeft.count == 4)
  }

  test("testCompareAFewDifferences") {
    val diffResult = returnDiff("Test1.txt", "Test3.txt")
    assert(diffResult.inLeftNotInRight.count == 2)
    assert(diffResult.inRightNotInLeft.count == 2)
  }

  test("testCompareTable1IsSubset") {
    val diffResult = returnDiff("Test4.txt", "Test1.txt")
    assert(diffResult.inLeftNotInRight.count == 0)
    assert(diffResult.inRightNotInLeft.count == 4)
  }

  test("testCompareTable2IsSubset") {
    val diffResult = returnDiff("Test1.txt", "Test5.txt")
    assert(diffResult.inLeftNotInRight.count == 5)
    assert(diffResult.inRightNotInLeft.count == 0)
  }
}