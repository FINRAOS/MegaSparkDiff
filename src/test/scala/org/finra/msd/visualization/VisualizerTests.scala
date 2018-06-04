package org.finra.msd.visualization

import org.finra.msd.basetestclasses.SparkSessionTrait
import org.scalatest.{FeatureSpec, Matchers}
import org.apache.commons.lang3.tuple
import org.apache.spark.sql.types.{DataTypes, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.finra.msd.basetestclasses.SparkSessionTrait
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, Matchers}
import org.finra.msd.implicits.DataFrameImplicits._
import org.finra.msd.sparkcompare.SparkCompare

class VisualizerTests  extends FeatureSpec  with SparkSessionTrait with Matchers{
  import sparkSession.implicits._
  import org.apache.spark.sql.functions._


  feature("Visualize as Text") {
    scenario("positive scenario for Visualize as Text") {
      pending
      val left = Seq(
        ("1", "1" , "A" ,"A"),
        ("2","2","B","B"),
        ("4","4","C","C"),
        ("5","5","D","D"),
        ("5","5","D","D"),
        ("6","6","E","E")
      ).toDF("key1" , "key2" , "value1" , "value2")

      val right = Seq(
        ("1","1",null,null),
        ("3","3","Y","Y"),
        ("5","5","D","D"),
        ("6","6","E","E"),
        (null,null,"zz","zz")
      ).toDF("key1" , "key2", "value1" , "value2")

      val comparisonReult: tuple.Pair[DataFrame, DataFrame] = SparkCompare.compareSchemaDataFrames(left,right)

      val key: Seq[String] = Seq("key1", "key2")

     //val dfVisual = Visualizer.dfVisualize(comparisonReult.getLeft,comparisonReult.getRight , key , 100)
      //dfVisual.show()
    }
  }
}
