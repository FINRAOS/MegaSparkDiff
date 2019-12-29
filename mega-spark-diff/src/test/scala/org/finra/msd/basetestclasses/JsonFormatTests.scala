package org.finra.msd.basetestclasses

import org.finra.msd.containers.DiffResult

trait JsonFormatTests {
  this: SparkFunSuiteDynamoDb =>
  def returnDiff(tableLeft: String, tableRight: String): DiffResult

  def testSameDataTypesJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypes") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testMixedDataTypesSimpleJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testMixedDataTypesSimple") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesWithListJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesWithList") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesWithListDiffJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesWithListDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesWithSetJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesWithSet") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesWithSetDiffJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesWithSetDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesWithMapJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesWithMap") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesWithMapDiffJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesWithMapDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesDiffJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 1) fail("Expected 1 difference coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesMixedColumnsSimpleDiffJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesMixedColumnsSimpleDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 1) fail("Expected 1 difference coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 1) fail("Expected 1 difference coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesExtraNullColumnJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesExtraNullColumn") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesExtraNullNestedListColumnJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesExtraNullNestedListColumn") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 1) fail("Expected 1 difference coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 1) fail("Expected 1 difference coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesExtraNullNestedMapColumnJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesExtraNullNestedMapColumn") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesWithMapListSetJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesWithMapListSet") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 0) fail("Expected 0 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 0) fail("Expected 0 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }

  def testSameDataTypesWithMapListSetDiffJsonFormat(tableLeft: String, tableRight: String): Unit = {
    test("testSameDataTypesWithMapListSetDiff") {
      val diffResult = returnDiff(tableLeft, tableRight)
      if (diffResult.inLeftNotInRight.count != 2) fail("Expected 2 differences coming from left table." + "  Instead, found " + diffResult.inLeftNotInRight.count + ".")
      if (diffResult.inRightNotInLeft.count != 2) fail("Expected 2 differences coming from right table." + "  Instead, found " + diffResult.inRightNotInLeft.count + ".")
    }
  }
}
