package org.finra.msd.sparkcompare

import java.io.{File, IOException}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.finra.msd.basetestclasses.SparkFunSuite
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
      .add("KEY", StringType, true)
      .add("l_VALUE", StringType, true)
      .add("l_RECORDREPEATCOUNT", LongType, true)
      .add("r_VALUE", StringType, true)
      .add("r_RECORDREPEATCOUNT", LongType, true)

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
      .add("l_VALUE1", StringType, true)
      .add("L_VALUE2", StringType, true)
      .add("l_RECORDREPEATCOUNT", IntegerType, true)
      .add("KEY1", StringType, true)
      .add("KEY2", StringType, true)
      .add("r_VALUE1", StringType, true)
      .add("r_VALUE2", StringType, true)
      .add("r_RECORDREPEATCOUNT", IntegerType, true)

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
    val file1Path = this.getClass.getClassLoader.getResource("TC5NullsAndEmptyData1.txt").getPath
    val file2Path = this.getClass.getClassLoader.getResource("TC5NullsAndEmptyData2.txt").getPath
    val diffResult = SparkCompare.compareFiles(file1Path, file2Path)

    if (diffResult.inLeftNotInRight.count == 0 || diffResult.inRightNotInLeft.count == 0)
      fail("Straightforward output of test results somehow failed")
  }

  test("testCompareFileSaveResults") {
    val file1Path = this.getClass.getClassLoader.getResource("Test4.txt").getPath
    val file2Path = this.getClass.getClassLoader.getResource("Test6.txt").getPath
    val testLoc = "file_test"
    cleanOutputDirectory("/" + testLoc)
    SparkCompare.compareFileSaveResults(file1Path, file2Path, outputDirectory + "/" + testLoc,
      true, ",")
    val outputFile = new File(outputDirectory + "/" + testLoc)
    assert (outputFile.exists == true ,"Output File Did Not Exist")
  }

  test("testCompareAppleTables") {
    val appleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA",
      "", "(select * from Persons1)", "table1")
    val diffResult = SparkCompare.compareAppleTables(appleTable, appleTable)

    assert(diffResult.inLeftNotInRight.count == 0)
    assert(diffResult.inRightNotInLeft.count == 0)
  }

  test("testCompareJDBCAppleTablesWithDifference") {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl,
      "SA", "", "(select * from Persons1 where personid=2)", "table1")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl,
      "SA", "", "(select * from Persons1 Where personid=1)", "table2")
    val diffResult = SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)

    //the expectation is one difference between both datasets
    assert(diffResult.inLeftNotInRight.count == 1)
    assert(diffResult.inRightNotInLeft.count == 1)
  }

  test("testCompareJDBCtpFileAppleTablesWithDifference") {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA", "",
      "(select * from Persons1)", "table1")
    val file1Path = this.getClass.getClassLoader.getResource("TC1DiffsAndDups1.txt").getPath
    val rightAppleTable = SparkFactory.parallelizeTextSource(file1Path, "table2")
    val diffResult = SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)

    assert(diffResult.inLeftNotInRight.count == 2)
    assert(diffResult.inRightNotInLeft.count == 5)
  }

  test("Compare JDBC to File AppleTables With Difference And Save To File") {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA",
      "", "(select * from Persons1)", "table1")
    val file1Path = this.getClass.getClassLoader.getResource("TC1DiffsAndDups1.txt").getPath
    val testLoc = "jdbc_test"
    val rightAppleTable = SparkFactory.parallelizeTextSource(file1Path, "table2")
    cleanOutputDirectory("/" + testLoc)

    SparkCompare.compareAppleTablesSaveResults(leftAppleTable, rightAppleTable, outputDirectory + "/"
      + testLoc, true, ",")

    val outputFile = new File(outputDirectory + "/" + testLoc)

    assert(outputFile.exists() == true)
  }

  test("test Compare Counts From JDBC Apple Tables With Difference") {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA", "",
      "(select * from Persons1)", "table1")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA", "",
      "(select * from test1)", "table2")
    val countResult = SparkCompare.compareAppleTablesCount(leftAppleTable, rightAppleTable)

    assert(countResult.getLeftCount != countResult.getRightCount)
  }

  private def cleanOutputDirectory(subdir: String) = {
    //will clean the output directory
    try FileUtils.deleteDirectory(new File(outputDirectory + subdir))
    catch {
      case e: IOException =>
        log.error("couldnt delete test output from prior run")
        log.error(e.toString)
    }
    FileUtil.createDirectory(outputDirectory + subdir)
  }

  test("test CSV Load"){
    val expectedSchema = new StructType()
      .add("fruit", StringType, true)
      .add("num_1", StringType, true)
      .add("num_2", StringType, true)
      .add("color", StringType, true)

    val file1Path: String = this.getClass.getClassLoader.getResource("TestCSV.txt").getPath
//  val leftAppleTable = SparkFactory.parallelizeCSVSource(file1Path, None, Option("|"), "left_table")
    val leftAppleTable = SparkFactory.parallelizeCSVSource(file1Path, Option(expectedSchema), Option("|"), "left_table")

    //printing info for the 'left' table
    val rowCollectionLeft: Array[Row] = leftAppleTable.getDataFrame.rdd.collect()
    println("ROW LENGTH: " + rowCollectionLeft{0}.length)

    for (row <- rowCollectionLeft)
    {
      println(row.toString())
      println(row.get(0))
    }

    leftAppleTable.getDataFrame.printSchema()
    println(leftAppleTable.getDataFrame)

    val file2Path: String = this.getClass.getClassLoader.getResource("TestCSV_2.txt").getPath
//  val rightAppleTable = SparkFactory.parallelizeCSVSource(file2Path, None, Option(","), "right_table")
    val rightAppleTable = SparkFactory.parallelizeCSVSource(file2Path, Option(expectedSchema), Option(","), "right_table")

    //printing info for the 'right' table
    val rowCollectionRight: Array[Row] = rightAppleTable.getDataFrame.rdd.collect()
    println("ROW LENGTH: " + rowCollectionRight{0}.length)

    for (row <- rowCollectionRight)
    {
      println(row.toString())
      println(row.get(0))
    }

    rightAppleTable.getDataFrame.printSchema()

    println(rightAppleTable.getDataFrame)

    //comparison complains about column name diffs between the tables (from the schemaStruct/inferSchema)
    val diffResult = SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)

    assert(diffResult.inLeftNotInRight.count == 0)
    assert(diffResult.inRightNotInLeft.count == 0)
  }
}