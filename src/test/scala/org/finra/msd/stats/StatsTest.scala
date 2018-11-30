package org.finra.msd.stats

import org.apache.spark.sql.DataFrame
import org.finra.msd.basetestclasses.SparkFunSuite
import org.finra.msd.containers.DiffResult
import org.finra.msd.sparkcompare.SparkCompare
import org.scalatest.BeforeAndAfterAll

class StatsTest extends SparkFunSuite with BeforeAndAfterAll {

  import testImplicits._

  test("Visualize as Text") {
    val left = Seq(
      ("1", "1" , "Adam" ,"Andreson"),
      ("2","2","Bob","Branson"),
      ("4","4","Chad","Charly"),
      ("5","5","Joe","Smith"),
      ("5","5","Joe","Smith"),
      ("6","6","Edward","Eddy"),
      ("7","7","normal","normal")
    ).toDF("key1" , "key2" , "value1" , "value2")

    val right   = Seq(
      ("3","3","Young","Yan"),
      ("5","5","Joe","Smith"),
      ("6","6","Edward","Eddy"),
      ("7","7","normal","normal"),
      (null,null,"null key","null key")
    ).toDF("key1" , "key2", "value1" , "value2")

    val comparisonResult: DiffResult = SparkCompare.compareSchemaDataFrames(left, right)
    val key: Seq[String] = Seq("key1", "key2")
    
    val joinedResults: DataFrame = comparisonResult.fullOuterJoinDataFrames(key)
    val stats = comparisonResult.discrepancyStats(key)
    
    joinedResults.show()
    println(stats)
  }
}
