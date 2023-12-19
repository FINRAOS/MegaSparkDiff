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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.StringType
import org.finra.msd.containers.{AppleTable, DiffResult}
import org.finra.msd.helpers.JsonHelper
import org.scalatest.Assertions.fail

import java.io.File
import java.nio.charset.StandardCharsets

class TestHelpers {

  def reportDiffs(diffResult: DiffResult,
                  expectedDiffs: Int): Unit = {
    reportDiffs(diffResult, expectedDiffs, expectedDiffs)
  }

  def reportDiffs(diffResult: DiffResult,
                  expectedDiffsLeftNotInRight: Int, expectedDiffsRightNotInLeft: Int): Unit = {
    if (diffResult.inLeftNotInRight.count != expectedDiffsLeftNotInRight)
      fail("Expected " + expectedDiffsLeftNotInRight +
        " differences coming from left dataframe." +
        "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
    if (diffResult.inRightNotInLeft.count != expectedDiffsRightNotInLeft)
      fail("Expected " + expectedDiffsRightNotInLeft +
        " differences coming from right dataframe." +
        "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
  }

  private def seqStringsToColumns(seq: Seq[String]): Seq[Column] = {
    seq.map(x => new Column(x))
  }

  def compareJdbcDataFrameSimpleToJsonFormat(appleTable: AppleTable, jsonFormatDf: DataFrame, keyColumns: Seq[String]): Unit = {
    val df = appleTable.getDataFrame

    val orderedColumns = df.schema.fieldNames.sorted

    val actualCount = jsonFormatDf.count()
    val expectedCount = df.count()

    assert(df.schema.fieldNames.length == jsonFormatDf.schema.fieldNames.length)
    assert(actualCount == expectedCount)
    val keyCols = seqStringsToColumns(keyColumns)
    val actualRows = jsonFormatDf.orderBy(keyCols:_*).take(actualCount.toInt)
    val expectedRows = df.orderBy(keyCols:_*).take(expectedCount.toInt)

    for (i <- 0 until expectedCount.toInt) {
      val actualRow = actualRows(i)
      val expectedRow = expectedRows(i)

      for (j <- 0 until expectedRow.length) {
        val columnName = orderedColumns(j)
        val actualFieldIndex = jsonFormatDf.schema.fieldIndex(columnName)
        val actual = actualRow.get(actualFieldIndex)
        val expectedFieldIndex = df.schema.fieldIndex(columnName)
        val expectedOriginal = expectedRow.get(expectedFieldIndex)
        val expected =
          if (jsonFormatDf.schema.fields(expectedFieldIndex).dataType == StringType && expectedOriginal != null) {
            if ((expectedOriginal.toString.startsWith("[") && expectedOriginal.toString.endsWith("]"))
              || (expectedOriginal.toString.startsWith("{") && expectedOriginal.toString.endsWith("}"))) expectedOriginal
            else "\"" + expectedOriginal + "\""
          } else expectedRow.get(expectedFieldIndex)

        if (actual == null) {
          assert(expected == null)
        } else if (actual.isInstanceOf[Array[Byte]] || expected.isInstanceOf[Array[Byte]]) {
          assert(actual.asInstanceOf[Array[Byte]].deep == expected.asInstanceOf[Array[Byte]].deep)
        } else {
          assert(actual.equals(expected))
        }
      }
    }
  }

  def compareJsonDataFrameActualToExpected(appleTable: AppleTable, jsonPath: String, txtPath: String, keyColumns: Seq[String], delimiter: String): Unit = {
    val df = appleTable.getDataFrame

    val jsonString = FileUtils.readFileToString(new File(jsonPath), StandardCharsets.UTF_8)
    val txtString = FileUtils.readFileToString(new File(txtPath), StandardCharsets.UTF_8)

    val expectedRowsJson = JsonHelper.jsonToMapList(jsonString)
    val expectedRowsTxt = txtString.split("\n")

    val orderedColumns = df.schema.fieldNames.sorted
    val expectedCount = expectedRowsJson.size()

    assert(expectedCount == expectedRowsTxt.length)
    assert(df.count() == expectedCount)
    val keyCols = seqStringsToColumns(keyColumns)
    val actualRows = df.orderBy(keyCols:_*).take(expectedCount)

    for (i <- 0 until expectedCount) {
      val actualRow = actualRows(i)
      val expectedRowJson = expectedRowsJson.get(i)
      val expectedRowTxt = expectedRowsTxt(i).split(delimiter)

      for (j <- 0 until expectedRowJson.size()) {
        val columnName = orderedColumns(j)
        val fieldIndex = df.schema.fieldIndex(columnName)
        val actual = actualRow.get(fieldIndex)
        val expectValJson = expectedRowJson.get(columnName)
        val expectValTxt = expectedRowTxt(j)

        if (actual == null) {
          assert(expectValJson == null)
          assert(expectValTxt == "")
        } else {
          assert(actual.equals(expectValJson))
          assert(actual.equals(expectValTxt))
        }
      }
    }
  }
}
