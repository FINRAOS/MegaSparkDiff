package org.finra.msd.sparkcompare

import org.apache.commons.lang3.tuple
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
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

  test("fullOuterJoin Multiple Keys and Columns")
  {
    val left = Seq(
      ("1", "1", "A", "A"),
      ("2", "2", "B", "B"),
      ("4", "4", "C", "C"),
      ("5", "5", "D", "D"),
      ("5", "5", "D", "D"),
      ("6", "6", "E", "E")
    ).toDF("key1", "key2", "value1", "value2")

    val right = Seq(
      ("1", "1", null, null),
      ("3", "3", "Y", "Y"),
      ("5", "5", "D", "D"),
      ("6", "6", "E", "E"),
      (null, null, "zz", "zz")
    ).toDF("key1", "key2", "value1", "value2")

    val comparisonReult: tuple.Pair[DataFrame, DataFrame] = SparkCompare.compareSchemaDataFrames(left, right)

    val key: Seq[String] = Seq("key1", "key2")
    val results: DataFrame = SparkCompare.fullOuterJoinDataFrames(comparisonReult.getLeft, comparisonReult.getRight, key)

    results.show()

    val expectedSchema = new StructType()
      .add("l_VALUE1", StringType, true)
      .add("L_VALUE2", StringType, true)
      .add("l_RECORDREPEATCOUNT", IntegerType, true)
      .add("KEY1", StringType, true)
      .add("KEY2", StringType, true)
      .add("r_VALUE1", StringType, true)
      .add("r_VALUE2", StringType, true)
      .add("r_RECORDREPEATCOUNT", IntegerType, true)

    val expected = Seq(
      Row(null, null, null, null, null, "zz", "zz", 1),
      Row("A", "A", 1, "1", "1", null, null, 1),
      Row("B", "B", 1, "2", "2", null, null, null),
      Row(null, null, null, "3", "3", "Y", "Y", 1),
      Row("C", "C", 1, "4", "4", null, null, null),
      Row("D", "D", 2, "5", "5", "D", "D", 1)
    ).toDf(sparkSession, expectedSchema)

    results.collect() should contain allElementsOf expected.collect()
  }
}