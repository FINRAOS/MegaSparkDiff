package org.finra.msd.containers

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import scala.beans.BeanProperty

/***
  * The container for the comparison result.
  * @param inLeftNotInRight the data set that contains the results in left but not in right
  * @param inRightNotInLeft the data set that contains the results in right but not in left
  */
case class DiffResult(@BeanProperty inLeftNotInRight: DataFrame, @BeanProperty inRightNotInLeft: DataFrame) {

  /**
    * Order the result by the provided columns.
    *
    * @param orderByCols is the column provided for order
    * @param isAsc       is the indicator for ascending or descending order.
    * @return the ordered data set
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
    * @return the data set without the columns
    */
  def removeCols(excludeCols: Array[String]) : DiffResult = {
    val left = inLeftNotInRight.drop(excludeCols:_*)
    val right = inRightNotInLeft.drop(excludeCols:_*)
    DiffResult(left, right)
  }

  /**
    * Indicating whether there is difference in the comparison.
    *
    * @return true is no difference; false if there is difference.
    */
  def noDiff(): Boolean = {
    if (inLeftNotInRight.count() == 0 && inRightNotInLeft.count() == 0) {
      return true
    }
    false
  }

}
