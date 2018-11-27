package org.finra.msd.containers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.beans.BeanProperty

case class DiffResult (@BeanProperty inLeftNotInRight :DataFrame, @BeanProperty inRightNotInLeft :DataFrame) {

  /**
    * This method does a full outer join between the resulting left and right DataFrames from the method
    * SparkCompare.compareSchemaDataFrames. It will return a single DataFrame having the left columns prefixed with l_
    * and the right columns prefixed with r_. the Key columns will not have prefixed. The resulting DataFrame will have
    * all l_ columns on the left, then the Key columns in the middle, then the r_ columns on the right.
    *
    * @param compositeKeyStrs a Sequence of Strings having the primary keys applicable for both dataframes
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

    //reselect the dataframes with prepended l. & r. to the columnss
    val prependedLeftDf: DataFrame = upperCaseLeft.toDF(prependedColumnsLeft: _*)
    val prependedRightDf: DataFrame = upperCaseRight.toDF(prependedColumnsRight: _*)


    val joinedDf: DataFrame = prependedLeftDf.as("l")
      .join(prependedRightDf.as("r"), compositeKeysUpperCase, "full_outer")

    val allColsWithKeysInTheMiddle: Seq[String] = nonKeyCols.map(c => "l_" + c) ++ compositeKeysUpperCase ++ nonKeyCols.map(c => "r_" + c)
    joinedDf.select(allColsWithKeysInTheMiddle.map(name => col(name)): _*)
  }
}
