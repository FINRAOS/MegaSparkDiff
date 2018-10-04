package org.finra.msd.visualization


import org.apache.commons.lang3.tuple
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.finra.msd.basetestclasses.SparkTestSuiteSessionTrait
import org.finra.msd.implicits.DataFrameImplicits._
import org.finra.msd.sparkcompare.SparkCompare
import org.scalatest.Matchers

class VisualizerTests extends SparkTestSuiteSessionTrait with Matchers {

  import sparkSession.implicits._


  test("Visualize as Text") {
    val left = Seq(
      ("key11", "key12", "A", "A"),
      ("key21", "key22", "B", "B"),
      ("4", "4", "C", "C"),
      ("5", "5", "D", "D"),
      ("5", "5", "D", "D"),
      ("6", "6", "E", "E")
    ).toDF("key1", "key2", "value1", "value2")

    val right = Seq(
      ("key11", "key12", null, null),
      ("3", "3", "Y", "Y"),
      ("5", "5", "D", "D"),
      ("6", "6", "E", "E"),
      (null, null, "zz", "zz")
    ).toDF("key1", "key2", "value1", "value2")

    val comparisonResult: tuple.Pair[DataFrame, DataFrame] = SparkCompare.compareSchemaDataFrames(left, right)

    val key: Seq[String] = Seq("key1", "key2")
    val joinedResults: DataFrame = SparkCompare.fullOuterJoinDataFrames(comparisonResult.getLeft, comparisonResult.getRight, key)
    Visualizer.renderHorizontalTable(joinedResults,100)

  }
}
