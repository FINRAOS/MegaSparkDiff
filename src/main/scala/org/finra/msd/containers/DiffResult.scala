package org.finra.msd.containers

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SQLImplicits}
import org.apache.spark.sql.functions._

import scala.beans.BeanProperty

case class DiffResult (@BeanProperty inLeftNotInRight :DataFrame, @BeanProperty inRightNotInLeft :DataFrame) {

  import org.finra.msd.sparkfactory.SparkFactory.sparkImplicits._
  
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

    //prepend l_ and r_ to nonkey columns
    val prependedColumnsLeft = compositeKeysUpperCase ++ nonKeyCols.map(c => "l_" + c).toSeq
    val prependedColumnsRight = compositeKeysUpperCase ++ nonKeyCols.map(c => "r_" + c).toSeq

    //reselect the DataFrames with prepended l_ & r_ to the columnss
    val prependedLeftDf: DataFrame = upperCaseLeft.toDF(prependedColumnsLeft: _*)
    val prependedRightDf: DataFrame = upperCaseRight.toDF(prependedColumnsRight: _*)

    val joinedDf: DataFrame = prependedLeftDf.as("l")
      .join(prependedRightDf.as("r"), compositeKeysUpperCase, "full_outer")

    val allColsWithKeysInTheMiddle: Seq[String] = nonKeyCols.map(c => "l_" + c) ++ compositeKeysUpperCase ++ nonKeyCols.map(c => "r_" + c)
    joinedDf.select(allColsWithKeysInTheMiddle.map(name => col(name)): _*)
  }

  /**
    * This method compares all "l_" with their corresponding "r_" columns from the joined table returned in
    * fullOuterJoinDataFrames() and returns a DataFrame that maps column names with the amount of discrepant entries
    * between those l_ and r_ columns. 
    *
    * @param compositeKeyStrs a Sequence of Strings having the primary keys applicable for both DataFrames
    * @return a DataFrame that maps between column names and the amount of discrepant entries for those "l_/r_" rows in
    *         the full outer joined table.
    */
  def discrepancyStats(compositeKeyStrs: Seq[String]): DataFrame = {

    val joinedDf: DataFrame = fullOuterJoinDataFrames(compositeKeyStrs)

    val compositeKeysUpperCase: Seq[String] = compositeKeyStrs.map(k => k.toUpperCase)
    val nonKeyCols: Seq[String] = inLeftNotInRight.columns.filter(c => !compositeKeysUpperCase.contains(c.toUpperCase)).toSeq.map(k => k.toUpperCase)
    val zColumns: Seq[String] = nonKeyCols.map( c => "z_" + c)
    
    //create new table with z_ columns that contain 0 if there the corresponding l_ and r_ columns were equal, 1 otherwise 
    var withEqFlags = joinedDf
    for ( c <- nonKeyCols )
      withEqFlags = withEqFlags.withColumn("z_" + c, when(withEqFlags("l_" + c) === withEqFlags("r_" + c), "0").otherwise("1"))

    //for each column, sum the corresponding z_ column to count how many discrepancies there were
    var counts:Map[String,Int] = Map()
    for ( c <- zColumns )
      counts += ( c.substring(2) -> withEqFlags.agg(sum(c)).first().getDouble(0).toInt )
    
    //sort the columns in decending order of how many discrepancies they had
    val problems:Seq[String] = nonKeyCols.sortWith(counts(_) > counts(_))
    
    //return a DataFrame containing the column discrepancy count information from above 
    var sortedCounts:Seq[(String,Int)] = Seq()
    for ( c <- problems )
      if ( !c.equals("RECORDREPEATCOUNT") ) // If table has keys, it shouldn't have duplicates, so this column is ignored
        sortedCounts = sortedCounts :+ (c,counts(c))
    sortedCounts.toDF("COLUMN_NAME","DISCREPANCIES")
  }
}
