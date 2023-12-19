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

package org.finra.msd.basetestclasses

import org.finra.msd.containers.DiffResult

trait JsonFormatTests {
  this: SparkFunSuite =>
  def returnDiff(tableLeft: String, tableRight: String, sameSchema: Boolean = false): DiffResult

  def testSameDataTypesComplexJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesComplex") {
      val expectedDiffs = 0
      val diffResult = returnDiff(tableLeft, tableRight)
      helpers.reportDiffs(diffResult, expectedDiffs)
    }
  }

  def testSameDataTypesComplexDiffValueJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesComplexDiffValue") {
      val expectedDiffs = 1
      val diffResult = returnDiff(tableLeft, tableRight)
      helpers.reportDiffs(diffResult, expectedDiffs)
    }
  }

  def testMixedDataTypesSimpleJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesSimple") {
      val expectedDiffs = 0
      val diffResult = returnDiff(tableLeft, tableRight)
      helpers.reportDiffs(diffResult, expectedDiffs)
    }
  }

  def testMixedDataTypesSimpleDiffJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesSimpleDiff") {
      val expectedDiffs = 2
      val diffResult = returnDiff(tableLeft, tableRight)
      helpers.reportDiffs(diffResult, expectedDiffs)
    }
  }

  def testSameDataTypesSimpleDiffMissingElementJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesSimpleDiffMissingElement") {
      val expectedDiffs = 1
      val diffResult = returnDiff(tableLeft, tableRight)
      helpers.reportDiffs(diffResult, expectedDiffs)
    }
  }

  def testSameDataTypesSimpleDiffExtraNullElementJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesSimpleDiffExtraNullElement") {
      val expectedDiffs = 0
      val diffResult = returnDiff(tableLeft, tableRight, sameSchema = true)
      helpers.reportDiffs(diffResult, expectedDiffs)

      val reason = "Expected \"Column Names Did Not Match\" exception."
      try {
        returnDiff(tableLeft, tableRight)
        fail(reason)
      }
      catch {
        case e: Exception =>
          if (!e.getMessage.equals("Column Names Did Not Match")) fail(reason)
        case _: Throwable =>
          fail(reason)
      }
    }
  }
}
