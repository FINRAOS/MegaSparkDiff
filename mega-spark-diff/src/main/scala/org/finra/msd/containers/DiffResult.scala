/*
 * Copyright 2017 MegaSparkDiff Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finra.msd.containers

import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SQLImplicits}
import org.apache.spark.sql.functions._

import scala.beans.BeanProperty

/***
 * The container for the comparison result.
 * @param inLeftNotInRight the data set that contains the results in left but not in right
 * @param inRightNotInLeft the data set that contains the results in right but not in left
 */
case class DiffResult(@BeanProperty inLeftNotInRight: DataFrame, @BeanProperty inRightNotInLeft: DataFrame) {

  import org.finra.msd.sparkfactory.SparkFactory.sparkImplicits._

  /**
   * Order the result by the provided columns.
   *
   * @param orderByCols is the column provided for order
   * @param isAsc       is the indicator for ascending or descending order.
   * @return            the ordered data set
   */
  def getOrderedResult(orderByCols: Array[String], isAsc: Boolean): DiffResult = {
    var cols: Array[Column] = orderByCols.map(str => col(str))
    if (!isAsc) {
      cols = orderByCols.map(str => col(str).desc)
    }
    val left = inLeftNotInRight.sort(cols: _*)
    val right = inRightNotInLeft.sort(cols: _*)
    DiffResult(left, right)
  }

  /**
   * Exclude some columns from the data set so that it won't be saved.
   *
   * @param excludeCols the column array that contains the columns to exclude from the data set
   * @return            the data set without the columns
   */
  def removeCols(excludeCols: Array[String]) : DiffResult = {
    val left = inLeftNotInRight.drop(excludeCols:_*)
    val right = inRightNotInLeft.drop(excludeCols:_*)
    DiffResult(left, right)
  }

  /**
   * Indicating whether there is difference in the comparison.
   *
   * @return true if there is no difference; false if there is difference.
   */
  def noDiff(): Boolean = {
    if (inLeftNotInRight.count() == 0 && inRightNotInLeft.count() == 0) {
      return true
    }
    false
  }
  
  /**
   * This method does a full outer join between the resulting left and right DataFrames from the method
   * SparkCompare.compareSchemaDataFrames. It will return a single DataFrame having the left columns prefixed with l_
   * and the right columns prefixed with r_. the Key columns will not have prefixed. The resulting DataFrame will have
   * all l_ columns on the left, then the Key columns in the middle, then the r_ columns on the right.
   *
   * @param compositeKeyStrs a Sequence of Strings having the primary keys applicable for both DataFrames
   * @return                 a DataFrame having the resulting full outer join operation.
   */
  def fullOuterJoinDataFrames(compositeKeyStrs: Seq[String]): DataFrame = {

    val compositeKeysUpperCaseSeq = compositeKeyStrs.map(k => k.toUpperCase)
    val compositeKeysUpperCase = compositeKeyStrs.map(k => k.toUpperCase).toSeq
    var tempLeft: DataFrame = inLeftNotInRight.select(inLeftNotInRight.columns.map(c => col(c).as(c.toUpperCase)): _*)
    var tempRight: DataFrame = inRightNotInLeft.select(inRightNotInLeft.columns.map(c => col(c).as(c.toUpperCase)): _*)

    for (col <- inLeftNotInRight.columns)
    {
      if (!compositeKeysUpperCaseSeq.contains(col.toUpperCase))
      {
        tempLeft = tempLeft.withColumnRenamed(col ,"l_" + col.toUpperCase)
      }
    }

    for (col <- inRightNotInLeft.columns)
    {
      if (!compositeKeysUpperCaseSeq.contains(col.toUpperCase))
      {
        tempRight = tempRight.withColumnRenamed(col ,"r_" + col.toUpperCase)
      }
    }
    val leftCols: Seq[String] = tempLeft.columns.filter(c => !compositeKeysUpperCase.contains(c.toUpperCase)).toSeq
    val rightCols: Seq[String] = tempRight.columns.filter(c => !compositeKeysUpperCase.contains(c.toUpperCase)).toSeq
    val joinedDf: DataFrame = tempLeft.as("l_")
      .join(tempRight.as("r_"), compositeKeysUpperCaseSeq, "full_outer")
    val allColsWithKeysInTheMiddle = leftCols.toSeq ++ compositeKeysUpperCase ++ rightCols.toSeq
    joinedDf.select( allColsWithKeysInTheMiddle.map(c => col(c)) :_*)
  }

  /**
   * This method compares all "l_" with their corresponding "r_" columns from the joined table returned in
   * fullOuterJoinDataFrames() and returns a DataFrame that maps column names with the amount of discrepant entries
   * between those l_ and r_ columns.
   *
   * @param compositeKeyStrs a Sequence of Strings having the primary keys applicable for both DataFrames
   * @return                 a DataFrame that maps between column names and the amount of discrepant entries for those "l_/r_" rows in
   *                         the full outer joined table.
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
