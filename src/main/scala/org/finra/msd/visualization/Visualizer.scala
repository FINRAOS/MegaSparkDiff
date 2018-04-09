package org.finra.msd.visualization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat, lit, when}
import org.finra.msd.sparkfactory.SparkFactory
import org.finra.msd.enums.VisualResultType

object Visualizer {

  /**
    * Generate template string which can be used by displayHTML() in databricks
    * @param left
    * @param right
    * @param composite_key_strs
    * @return a template string
    */
  def visualize(left: DataFrame, right: DataFrame, composite_key_strs: Seq[String]): String = {
    try{
      if(left == null) {
        throw new Exception("Left table is null")
      } else if(right == null) {
        throw new Exception("Right table is null")
      } else if(composite_key_strs == null || composite_key_strs.isEmpty) {
        throw  new Exception("Please specify primary/composite key")
      }

      val headerRows = generateHeadersRows(left, right, composite_key_strs);

      val header = headerRows._1;
      val rows = headerRows._2;
      val visualResultType = headerRows._3;

      val visualizerTemplate: String = s"""<table>
        <tr>
          ${header.map(h => s"<th>${h}</th>").mkString}
        </tr>
        <td>
          ${rows.map {
        row =>
          s"<tr>${row.map {c => {
            if(c.contains("<==>")){
              val lr: Array[String] = c.split("<==>")
              s"<td><span style='color:blue;'>${if(lr(0) == "") "NONE" else lr(0)}</span></br><span style='color:red;'>${if(lr(1) == "") "NONE" else lr(1)}</span></td>"
            } else {
              if(visualResultType == VisualResultType.LEFT) {
                s"<td><span style='color:blue;>${c}</span></td>"
              } else if(visualResultType == VisualResultType.RIGHT) {
                s"<td><span style='color:red;>${c}</span></td>"
              } else {
                s"<td>${c}</td>"
              }
            }
          }}}</tr>"
      }}
        </td>
        </table>"""

      visualizerTemplate
    } catch {
      case ex: Exception => print(ex.getMessage)
    }

    null
  }

  /**
    * Generate header, rows and a flag indicating the following scenarios:
    *   1 - right is empty
    *   2 - left is empty
    *   3 - both are not empty
    * @param left
    * @param right
    * @param composite_key_strs: a sequence of columns to make up a composite key
    * @return a tuple containing header, rows and visualResultType
    */
  def generateHeadersRows(left: DataFrame, right: DataFrame, composite_key_strs: Seq[String]) : (Seq[String], Seq[Seq[String]], VisualResultType) = {
    var vrt: VisualResultType = null;

    //if left is empty, visualResultType = VisualResultType.RIGHT
    //if right is empty, visualResultType = VisualResultType.LEFT
    //if neither is empty, visualResultType = VisualResultType.BOTH
    //if both are empty, no need to call visualize method???
    if(left.count() == 0) {
      vrt = VisualResultType.RIGHT;
    } else if(right.count() == 0) {
      vrt = VisualResultType.LEFT;
    } else {
      vrt = VisualResultType.BOTH;
    }

    val composite_keys_col = composite_key_strs.map(c => col(c)).toList

    val non_key_cols = left.columns.filter(c => !composite_key_strs.contains(c)).toList

    val joinedRdd = left.as("l")
      .join(right.as("r"), composite_key_strs)
      .select(composite_keys_col:::non_key_cols.map(mapHelper):_*)

    //get all rows from dataframe
    val data = joinedRdd.collect()

    val header = joinedRdd.schema.fieldNames.toSeq
    val rows: Seq[Seq[String]] = data.map {row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case _ => cell.toString
        }
        str
      }: Seq[String]
    }

    (header, rows, vrt)
  }

  /**
    * Helper method to map non-composite key cell value to desired value
    * @param value: column name
    * @return
    */
  def mapHelper(value: String) = {
    val x = SparkFactory.sparkSession
    import x.implicits._

    //"<==>" is used as delimiter for mismatched left value and right value
    when($"l.$value".isNull && $"r.$value".isNull, lit("null value"))
      .when($"l.$value".isNull, concat(lit("null value<==>"), $"r.$value"))
      .when($"r.$value".isNull, concat($"l.$value", lit("<==>null value")))
      .when($"l.$value" === $"r.$value", $"l.$value")
      .otherwise(concat($"l.$value",lit("<==>"),$"r.$value"))
      .as(value)
  }
}
