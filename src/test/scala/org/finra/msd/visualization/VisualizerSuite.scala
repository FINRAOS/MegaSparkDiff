package org.finra.msd.visualization


import org.apache.spark.sql.DataFrame
import org.finra.msd.basetestclasses.SparkFunSuite
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkcompare.SparkCompare
import org.scalatest.BeforeAndAfterAll

class VisualizerSuite extends SparkFunSuite with BeforeAndAfterAll {

  import testImplicits._

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

    val comparisonResult: DiffResult = SparkCompare.compareSchemaDataFrames(left, right)

    val key: Seq[String] = Seq("key1", "key2")
    val joinedResults: DataFrame = SparkCompare.fullOuterJoinDataFrames(comparisonResult.inLeftNotInRight,
      comparisonResult.inRightNotInLeft, key)
    val html = Visualizer.renderHorizontalTable(joinedResults, 100)

    assert(html.contains("class='different'"))
    assert(html.contains("class='same'"))
    assert(html.contains("</html>"))
  }
}
