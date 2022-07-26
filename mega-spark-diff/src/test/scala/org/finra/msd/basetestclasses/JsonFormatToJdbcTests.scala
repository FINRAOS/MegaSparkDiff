package org.finra.msd.basetestclasses

import org.finra.msd.containers.DiffResult

trait JsonFormatToJdbcTests {
  this: SparkFunSuiteDynamoDb =>
  def returnDiff(tableLeft: String, tableRight: String): DiffResult

  def testSameDataTypesJsonFormatToJdbc(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypes") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesSimpleJsonFormatToJdbc(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesSimple") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesDiffJsonFormatToJdbc(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 1) fail("Expected 1 difference coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesSimpleDiffJsonFormatToJdbc(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesSimpleDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 1) fail("Expected 1 difference coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesWithListDiffJsonFormatToJdbc(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesWithListDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesWithSetDiffJsonFormatToJdbc(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesWithSetDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesWithMapDiffJsonFormatToJdbc(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesWithMapDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesExtraNullColumnJsonFormatToJdbc(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesExtraNullColumn") {
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
