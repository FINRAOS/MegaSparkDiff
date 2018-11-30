package org.finra.msd.containers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.beans.BeanProperty

case class DiffResult (@BeanProperty inLeftNotInRight :DataFrame, @BeanProperty inRightNotInLeft :DataFrame) {

  /**
    * This method does a full outer join between the resulting left and right DataFrames from the method
    * SparkCompare.compareSchemaDataFrames. It will return a single DataFrame having the left columns prefixed with l_
    * and the right columns prefixed with r_. the Key columns will not have prefixed. The resulting DataFrame will have
    * all l_ columns on the left, then the Key columns in the middle, then the r_ columns on the right.
    *
    * @param compositeKeyStrs a Sequence of Strings having the primary keys applicable for both DataFrames
    * @return a DataFrame having the resulting full outer join operation.
    */
  def fullOuterJoinDataFrames(compositeKeyStrs: Seq[String]): DataFrame = {

    //convert column names to uppercase
    val upperCaseLeft: DataFrame = inLeftNotInRight.toDF(inLeftNotInRight.columns.map(_.toUpperCase): _*)
    val upperCaseRight: DataFrame = inRightNotInLeft.toDF(inRightNotInLeft.columns.map(_.toUpperCase): _*)

    val compositeKeysUpperCase: Seq[String] = compositeKeyStrs.map(k => k.toUpperCase)
    val nonKeyCols: Seq[String] = upperCaseLeft.columns.filter(c => !compositeKeysUpperCase.contains(c)).toSeq

    //prepend l and r to nonkey columns
    val prependedColumnsLeft = compositeKeysUpperCase ++ nonKeyCols.map(c => "l_" + c).toSeq
    val prependedColumnsRight = compositeKeysUpperCase ++ nonKeyCols.map(c => "r_" + c).toSeq

    //reselect the DataFrames with prepended l. & r. to the columnss
    val prependedLeftDf: DataFrame = upperCaseLeft.toDF(prependedColumnsLeft: _*)
    val prependedRightDf: DataFrame = upperCaseRight.toDF(prependedColumnsRight: _*)

    val joinedDf: DataFrame = prependedLeftDf.as("l")
      .join(prependedRightDf.as("r"), compositeKeysUpperCase, "full_outer")

    val allColsWithKeysInTheMiddle: Seq[String] = nonKeyCols.map(c => "l_" + c) ++ compositeKeysUpperCase ++ nonKeyCols.map(c => "r_" + c)
    joinedDf.select(allColsWithKeysInTheMiddle.map(name => col(name)): _*)
  }

  /**
    * This method compares all "l_" with their corresponding "r_" columns from the joined table returned in
    * fullOuterJoinDataFrames() and returns a mapping between column and the amount/percentage of discrepant
    * entries in that column. 
    *
    * @param compositeKeyStrs a Sequence of Strings having the primary keys applicable for both DataFrames
    * @return a Map between column names and the amount/percentage of discrepant entries for that row.  The key values
    *         are a Seq containing two Doubles: amount and percentage (ranging from 0 to 1).
    */
  def discrepancyStats(compositeKeyStrs: Seq[String]): Map[String,Seq[Double]] = {

    val joinedDf: DataFrame = fullOuterJoinDataFrames(compositeKeyStrs)

    val compositeKeysUpperCase: Seq[String] = compositeKeyStrs.map(k => k.toUpperCase)
    val nonKeyCols: Seq[String] = inLeftNotInRight.columns.filter(c => !compositeKeysUpperCase.contains(c.toUpperCase)).toSeq.map(k => k.toUpperCase)
    val zColumns: Seq[String] = nonKeyCols.map( c => "z_" + c)
    
    var withEqFlags = joinedDf
    for ( c <- nonKeyCols )
      withEqFlags = withEqFlags.withColumn("z_" + c, when(withEqFlags("l_" + c) === withEqFlags("r_" + c), "1").otherwise("0"))

    var counts:Map[String,Seq[Double]] = Map()
    for ( c <- zColumns ) {
      val s: Double = withEqFlags.agg(sum(c)).first().getDouble(0)
      val a: Double = withEqFlags.agg(avg(c)).first().getDouble(0)
      counts += ( c.substring(2) -> Seq(s,a) )
    }
    
    val problems:Seq[String] = nonKeyCols.sortWith(counts(_).head < counts(_).head)
    var sortedCounts:Map[String,Seq[Double]] = Map()
    for ( c <- problems ) {
      sortedCounts += ( c -> counts(c) )
    }
    return sortedCounts
  }
}
