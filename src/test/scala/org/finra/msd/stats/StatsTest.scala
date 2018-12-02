package org.finra.msd.stats

import org.apache.spark.sql.DataFrame
import org.finra.msd.basetestclasses.SparkFunSuite
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkcompare.SparkCompare
import org.scalatest.BeforeAndAfterAll

class StatsTest extends SparkFunSuite with BeforeAndAfterAll {

  import testImplicits._

  test("Way too many discrepancies") {
    
    val key: Seq[String] = Seq("key1", "key2")

    val left = Seq(
      ("1","1","Adam","Andreson"),
      ("2","2","Bob","Branson"),
      ("4","4","Chad","Charly"),
      ("5","5","Joe","Smith"),
      ("5","5","Joe","Smith"),
      ("6","6","Edward","Eddy"),
      ("7","7","normal","normal")
    ).toDF("key1" , "key2" , "value1" , "value2")

    val right = Seq(
      ("3","3","Young","Yan"),
      ("5","5","Joe","Smith"),
      ("6","6","Edward","Eddy"),
      ("7","7","normal","normal"),
      (null,null,"null key","null key")
    ).toDF("key1" , "key2", "value1" , "value2")
    
    val comparisonResult: DiffResult = SparkCompare.compareSchemaDataFrames(left, right)
    val joinedResults: DataFrame = comparisonResult.fullOuterJoinDataFrames(key)
    val stats = comparisonResult.discrepancyStats(key)
    
    joinedResults.show()
    stats.show()
  }

  test("A few discrepancies across two non-key columns") {
    
    val key: Seq[String] = Seq("a_column")

    val left = Seq(
      ("a1","b1","c1","d1"),
      ("a2","b2","c2","d2"),
      ("a3","b3","c3","d3"),
      ("a4","b4","c4","d4"),
      ("a5","b5","c5","d5"),
      ("a6","b6","c6","d6"),
      ("a7","b7","c7","d7")
    ).toDF("a_column","b_column","c_column","d_column")

    val right = Seq(
      ("a1","b1","c1","d1"),
      ("a2","b2","c2","x2"),
      ("a3","b3","c3","d3"),
      ("a4","x4","c4","d4"),
      ("a5","b5","c5","d5"),
      ("a6","b6","c6","d6"),
      ("a7","x7","c7","d7")
    ).toDF("a_column","b_column","c_column","d_column")
    
    val comparisonResult: DiffResult = SparkCompare.compareSchemaDataFrames(left, right)
    val joinedResults: DataFrame = comparisonResult.fullOuterJoinDataFrames(key)
    val stats = comparisonResult.discrepancyStats(key)

    joinedResults.show()
    stats.show()
  }

  test("Discrepancies in the key columns") {

    val key: Seq[String] = Seq("a_column")

    val left = Seq(
      ("a1","b1","c1","d1"),
      ("a2","b2","c2","d2"),
      ("a3","b3","c3","d3"),
      ("a4","b4","c4","d4"),
      ("a5","b5","c5","d5"),
      ("a6","b6","c6","d6"),
      ("a7","b7","c7","d7")
    ).toDF("a_column","b_column","c_column","d_column")

    val right = Seq(
      ("a1","b1","c1","d1"),
      ("a2","b2","c2","d2"),
      ("a3","b3","c3","d3"),
      ("a8","b4","c4","d4"),
      ("a5","b5","c5","d5"),
      ("a6","b6","c6","d6"),
      ("a9","b7","c7","d7")
    ).toDF("a_column","b_column","c_column","d_column")

    val comparisonResult: DiffResult = SparkCompare.compareSchemaDataFrames(left, right)
    val joinedResults: DataFrame = comparisonResult.fullOuterJoinDataFrames(key)
    val stats = comparisonResult.discrepancyStats(key)

    joinedResults.show()
    stats.show()
  }
}
