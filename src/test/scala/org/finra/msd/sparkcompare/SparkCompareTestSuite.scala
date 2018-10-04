package org.finra.msd.sparkcompare

import org.apache.commons.lang3.tuple
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.finra.msd.basetestclasses.SparkTestSuiteSessionTrait
import org.finra.msd.implicits.DataFrameImplicits._
import org.scalatest.Matchers

class SparkCompareTestSuite extends SparkTestSuiteSessionTrait with Matchers
{

  import sparkSession.implicits._

  test("FullOuterJoin") {
      val left = Seq(
        ("1","A"),
        ("2","B"),
        ("4","C"),
        ("5","D"),
        ("5","D"),
        ("6","E")
      ).toDF("key" , "value")

      val right = Seq(
        ("1",null),
        ("3","Y"),
        ("5","D"),
        ("6","E"),
        (null , "zz")
      ).toDF("key" , "value")

      val comparisonReult: tuple.Pair[DataFrame, DataFrame] = SparkCompare.compareSchemaDataFrames(left,right)

      val key: Seq[String] = Seq("key")
      val joinedDf = SparkCompare.fullOuterJoinDataFrames(comparisonReult.getLeft,comparisonReult.getRight,key)

      val expectedSchema = new StructType()
        .add("KEY",StringType , true)
        .add("l_VALUE" , StringType,true)
        .add("l_RECORDREPEATCOUNT" , LongType , true)
        .add("r_VALUE" , StringType , true)
        .add("r_RECORDREPEATCOUNT" , LongType , true)

      val expected = Seq(
        Row(null,null,null,"zz",1L),
        Row("1","A",1L,null,1L),
        Row("2","B",1L,null,null),
        Row("3",null,null,"Y",1L),
        Row("4","C",1L,null,null),
        Row("5","D",2L,"D",1L)
      ).toDf(sparkSession = sparkSession , expectedSchema)

      joinedDf.collect() should contain allElementsOf expected.collect()
    }
}