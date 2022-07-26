package org.finra.msd.basetestclasses

import org.finra.msd.containers.DiffResult

trait JsonFormatToFileTests {
  this: SparkFunSuiteDynamoDb =>
  def returnDiff(tableLeft: String, tableRight: String): DiffResult

  def testSameDataTypesJsonFormatToFile(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypes") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesSimpleJsonFormatToFile(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesSimple") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesDiffJsonFormatToFile(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 1) fail("Expected 1 difference coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesSimpleDiffJsonFormatToFile(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesSimpleDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 1) fail("Expected 1 difference coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesWithListDiffJsonFormatToFile(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesWithListDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesWithSetDiffJsonFormatToFile(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesWithSetDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesWithMapDiffJsonFormatToFile(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesWithMapDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesExtraNullColumnJsonFormatToFile(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesExtraNullColumn") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }
}
