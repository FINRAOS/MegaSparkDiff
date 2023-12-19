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

import java.io.{File, IOException}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.finra.msd.basetestclasses.SparkFunSuite
import org.finra.msd.memorydb.MemoryDbHsql
import org.finra.msd.sparkfactory.SparkFactory
import org.finra.msd.util.FileUtil

class SparkCompareSuite extends SparkFunSuite {

  import testImplicits._

  test("FullOuterJoin") {
    val left = Seq(
      ("1", "A"),
      ("2", "B"),
      ("4", "C"),
      ("5", "D"),
      ("5", "D"),
      ("6", "E")
    ).toDF("key", "value")

    val right = Seq(
      ("1", null),
      ("3", "Y"),
      ("5", "D"),
      ("6", "E"),
      (null, "zz")
    ).toDF("key", "value")

    val comparisonReult = SparkCompare.compareSchemaDataFrames(left, right)

    val key: Seq[String] = Seq("key")
    val joinedDf = comparisonReult.fullOuterJoinDataFrames(key)

    val expectedSchema = new StructType()
      .add("KEY", StringType, nullable = true)
      .add("l_VALUE", StringType, nullable = true)
      .add("l_RECORDREPEATCOUNT", LongType, nullable = true)
      .add("r_VALUE", StringType, nullable = true)
      .add("r_RECORDREPEATCOUNT", LongType, nullable = true)

    val expected = Seq(
      Row(null, null, null, "zz", 1L),
      Row("1", "A", 1L, null, 1L),
      Row("2", "B", 1L, null, null),
      Row("3", null, null, "Y", 1L),
      Row("4", "C", 1L, null, null),
      Row("5", "D", 2L, "D", 1L)
    ).toDf(expectedSchema).select('l_value, 'l_recordrepeatcount, 'key, 'r_value, 'r_recordrepeatcount)

    joinedDf.collect() should contain allElementsOf expected.collect()
  }

  test("fullOuterJoin Multiple Keys and Columns") {
    val left = Seq(
      ("1", "1", "A", "A"),
      ("2", "2", "B", "B"),
      ("4", "4", "C", "C"),
      ("5", "5", "D", "D"),
      ("5", "5", "D", "D"),
      ("6", "6", "E", "E")
    ).toDF("key1", "key2", "value1", "value2")

    val right = Seq(
      ("1", "1", null, null),
      ("3", "3", "Y", "Y"),
      ("5", "5", "D", "D"),
      ("6", "6", "E", "E"),
      (null, null, "zz", "zz")
    ).toDF("key1", "key2", "value1", "value2")

    val diffResult = SparkCompare.compareSchemaDataFrames(left, right)

    val key: Seq[String] = Seq("key1", "key2")
    val results: DataFrame = diffResult.fullOuterJoinDataFrames(key)

    val expectedSchema = new StructType()
      .add("l_VALUE1", StringType, nullable = true)
      .add("L_VALUE2", StringType, nullable = true)
      .add("l_RECORDREPEATCOUNT", IntegerType, nullable = true)
      .add("KEY1", StringType, nullable = true)
      .add("KEY2", StringType, nullable = true)
      .add("r_VALUE1", StringType, nullable = true)
      .add("r_VALUE2", StringType, nullable = true)
      .add("r_RECORDREPEATCOUNT", IntegerType, nullable = true)

    val expected = Seq(
      Row(null, null, null, null, null, "zz", "zz", 1),
      Row("A", "A", 1, "1", "1", null, null, 1),
      Row("B", "B", 1, "2", "2", null, null, null),
      Row(null, null, null, "3", "3", "Y", "Y", 1),
      Row("C", "C", 1, "4", "4", null, null, null),
      Row("D", "D", 2, "5", "5", "D", "D", 1)
    ).toDf(expectedSchema)

    results.collect() should contain allElementsOf expected.collect()
  }

  test("testCompareRdd") { //code to get file1 location
    val file1Path = this.getClass.getClassLoader.getResource("txt/TC5NullsAndEmptyData1.txt").getPath
    val file2Path = this.getClass.getClassLoader.getResource("txt/TC5NullsAndEmptyData2.txt").getPath
    val diffResult = SparkCompare.compareFiles(file1Path, file2Path)

    if (diffResult.inLeftNotInRight.count == 0 || diffResult.inRightNotInLeft.count == 0)
      fail("Straightforward output of test results somehow failed")
  }

  test("testCompareFileSaveResults") {
    val file1Path = this.getClass.getClassLoader.getResource("txt/Fruit4.txt").getPath
    val file2Path = this.getClass.getClassLoader.getResource("txt/Fruit6.txt").getPath
    val testLoc = "file_test"
    cleanOutputDirectory("/" + testLoc)
    SparkCompare.compareFileSaveResults(file1Path, file2Path, outputDirectory + "/" + testLoc,
      singleFileOutput = true, ",")
    val outputFile = new File(outputDirectory + "/" + testLoc)
    assert (outputFile.exists ,"Output File Did Not Exist")
  }

  test("testCompareAppleTables") {
    val expectedDiffs = 0
    val appleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA",
      "", "(select * from Persons1)", "table1")
    val diffResult = SparkCompare.compareAppleTables(appleTable, appleTable)

    helpers.reportDiffs(diffResult, expectedDiffs)
  }

  test("testCompareJDBCAppleTablesWithDifference") {
    val expectedDiffs = 1
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl,
      "SA", "", "(select * from Persons1 where personid=2)", "table1")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl,
      "SA", "", "(select * from Persons1 Where personid=1)", "table2")
    val diffResult = SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)

    //the expectation is one difference between both datasets
    helpers.reportDiffs(diffResult, expectedDiffs)
  }

  test("testCompareJDBCtpFileAppleTablesWithDifference") {
    val expectedDiffsLeftNotInRight = 2
    val expectedDiffsRightNotInLeft = 5
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from Persons1)", "table1")
    val file1Path = this.getClass.getClassLoader.getResource("txt/TC1DiffsAndDups1.txt").getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(file1Path, "table2")
    val diffResult = SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)

    helpers.reportDiffs(diffResult, expectedDiffsLeftNotInRight, expectedDiffsRightNotInLeft)
  }

  test("Compare JDBC to File AppleTables With Difference And Save To File") {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA",
      "", "(select * from Persons1)", "table1")
    val file1Path = this.getClass.getClassLoader.getResource("txt/TC1DiffsAndDups1.txt").getPath
    val testLoc = "jdbc_test"
    val rightAppleTable = SparkFactory.parallelizeTextSource(file1Path, "table2")
    cleanOutputDirectory("/" + testLoc)

    SparkCompare.compareAppleTablesSaveResults(leftAppleTable, rightAppleTable, outputDirectory + "/"
      + testLoc, singleFileOutput = true, ",")

    val outputFile = new File(outputDirectory + "/" + testLoc)

    assert(outputFile.exists())
  }

  test("test Compare Counts From JDBC Apple Tables With Difference") {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from Persons1)", "table1")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from Fruit1)", "table2")
    val countResult = SparkCompare.compareAppleTablesCount(leftAppleTable, rightAppleTable)

    assert(countResult.getLeftCount != countResult.getRightCount)
  }

  private def cleanOutputDirectory(subdir: String): Unit = {
    //will clean the output directory
    try FileUtils.deleteDirectory(new File(outputDirectory + subdir))
    catch {
      case e: IOException =>
        log.error("could not delete test output from prior run")
        log.error(e.toString)
    }
    FileUtil.createDirectory(outputDirectory + subdir)
  }
}