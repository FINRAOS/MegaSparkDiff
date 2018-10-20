package org.finra.msd.visualization

import java.util

import org.apache.spark.sql.{Dataset, Row}
import org.finra.msd.basetestclasses.SparkFunSuite
import org.finra.msd.sparkcompare.SparkCompare
import org.finra.msd.sparkfactory.SparkFactory

class VisualizerTest extends SparkFunSuite {
  test("basicVisualizerTest") {
    val diffResult = getAppleTablediffResult("Test6", "Test7")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FRUIT", 100)
    if (html.isEmpty) fail("html was empty")
  }

  test("emptyLeftDfTest") {
    val diffResult = getAppleTablediffResult("Test4", "Test1")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FRUIT", 100)
    if (html.isEmpty) fail("html was empty")
  }

  test("emptyRightDfTest") {
    val diffResult = getAppleTablediffResult("Test1", "Test4")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FRUIT", 100)
    if (html.isEmpty) fail("html was empty")
  }

  test("nullLeftDfTest") {
    val diffResult = getAppleTablediffResult("Test1", "Test4")
    val html = generateString(null, diffResult.inRightNotInLeft, "FRUIT", 100)
    assert("<h3>Error message: Left dataframe is null</h3>" == html)
  }

  test("nullRightDfTest") {
    val diffResult = getAppleTablediffResult("Test1", "Test4")
    val html = generateString(diffResult.inLeftNotInRight, null, "FRUIT", 100)
    assert("<h3>Error message: Right dataframe is null</h3>" == html)
  }

  test("emptyKeyTest") {
    val diffResult = getAppleTablediffResult("Test1", "Test4")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "", 100)
    assert("<h3>Error message: One or more keys is empty or null</h3>" == html)
  }

  test("nullKeyTest") {
    val diffResult = getAppleTablediffResult("Test1", "Test4")
    val html = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, null, 100)
    assert("<h3>Error message: One or more keys is empty or null</h3>" == html)
  }

  test("keyCaseTest") {
    val diffResult = getAppleTablediffResult("Test1", "Test4")
    var flag = true
    var result1 = ""
    var result2 = ""
    try {
      result1 = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "Fruit", 100)
      result2 = generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FrUit", 100)
    } catch {
      case ex: Exception =>
        flag = false
    }
    assert(true == flag)
    assert(result1 == result2)
  }

  test("invalidMaxRecordsTest") {
    val diffResult = getAppleTablediffResult("Test1", "Test4")
    var flag = true
    try generateString(diffResult.inLeftNotInRight, diffResult.inRightNotInLeft, "FRUIT", -100)
    catch {
      case ex: Exception =>
        flag = false
    }
    assert(true == flag)
  }

  private def generateString(left: Dataset[Row], right: Dataset[Row], key: String, maxRecords: Int) = {
    //Primary Key as Java List
    val primaryKeySeq = Seq(key)
    val html = Visualizer.generateVisualizerTemplate(left, right, primaryKeySeq, maxRecords)
    html
  }

  private def getAppleTablediffResult(testName1: String, testName2: String) = {
    val leftAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA", "",
      "(select * from " + testName1 + ")", "table1")
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(hsqlDriverName, hsqlUrl, "SA", "",
      "(select * from " + testName2 + ")", "table2")
    SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable)
  }
}