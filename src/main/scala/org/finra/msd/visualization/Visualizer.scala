package org.finra.msd.visualization

import org.apache.spark.sql.functions.{col, concat, lit, when}
import org.apache.spark.sql._
import org.finra.msd.sparkfactory.SparkFactory
import org.finra.msd.enums.VisualResultType
import org.finra.msd.customExceptions._

object Visualizer {

  /**
    * Generate template string which can be used by displayHTML() in databricks
    *
    * @param left
    * @param right
    * @param composite_key_strs
    * @return a template string
    */
  def generateVisualizerTemplate(left: DataFrame, right: DataFrame, composite_key_strs: Seq[String] , maxRecords: Integer = 1000): String = {
    var visualizerTemplate: String = "really?"

    try{
      require(left != null, throw new DataFrameNullException("Left table is null"));
      require(right != null, throw new DataFrameNullException("Right table is null"));
      require(composite_key_strs != null && !composite_key_strs.isEmpty, throw new JoinKeysNullException(
                    "Please specify primary/composite key"));
      require(isValidKey(composite_key_strs), throw new InValidKeyException("One or more keys is empty or null"))

      val headerRows = generateHeadersRows(left, right, composite_key_strs , maxRecords);

      val header = headerRows._1;
      val rows = headerRows._2;
      val visualResultType = headerRows._3;

      visualizerTemplate = s"""
      <!DOCTYPE html>
      <head>
        <meta charset="utf-8">
        <style>
          body {
            font: 15px Lato, sans-serif;
            font-weight: lighter;
          }

          table, tr, th, td {
            margin: 0 auto;
            max-width: 1024px;
            border:1px solid black;
            padding: 5px;
            text-align: center
          }

          th, td {
            min-width: 60px;
          }

          table {
            margin-top: 30px;
            border-collapse: collapse;
          }

          th {
            background: #65BDF0;
          }

          tr:nth-child(even) {
            background: #D0E4F5;
          }

          tr:nth-child(odd) {
            background: #F2F3F3;
          }

          .spanBlue {
            color: blue;
          }

          .spanRed {
            color: red
          }
        </style>
      </head>
      <body>
      <table>
        <tr>
          ${header.map(h => s"<th>${h}</th>").mkString}
        </tr>
        ${
        if(visualResultType == VisualResultType.LEFT) {
          rows.map {
            row =>
              s"<tr>" +
                s"${ row.map (c =>
                                if(c.contains("<==>")) {
                                    val lr: Array[String] = c.split("<==>")

                                    s"<td>" +
                                    s"<span class='spanBlue'>${lr(0)}</span>" +
                                    s"</td>"
                                } else {
                                    s"<td>" +
                                    s"<span class='spanBlue'>${c}</span>" +
                                          s"</td>"
                                }
                )}</tr>"
          }
        } else if(visualResultType == VisualResultType.RIGHT) {
          rows.map {
            row =>
              s"<tr>" +
                s"${row.map {c =>
                  if(c.contains("<==>")) {
                    val lr: Array[String] = c.split("<==>")

                    s"<td>" +
                      s"<span class='spanRed'>${lr(1)}</span>" +
                      s"</td>"
                  } else {
                    s"<td>" +
                      s"<span class='spanRed'>${c}</span>" +
                      s"</td>"
                  }
                }}</tr>"
          }
        } else{
          rows.map {
            row =>
              s"<tr>${row.map (c => {
                if(c.contains("<==>")){
                  val lr: Array[String] = c.split("<==>")
                  s"<td>" +
                    s"<span class='spanBlue'>${if(lr(0) == "") "(empty)" else lr(0)}</span></br>" +
                    s"<span class='spanRed'>${if(lr(1) == "") "(empty)" else lr(1)}</span>" +
                    s"</td>"
                } else {
                  s"<td>${c}</td>"
                }
              })}</tr>"
          }
        }
      }
        </table>
        </body>
        </html>
        """
    } catch {
      case ex: DataFrameNullException => visualizerTemplate = "Error message: " + ex.getMessage
      case ex: JoinKeysNullException => visualizerTemplate = "Error message: " + ex.getMessage
      case ex: SparkSessionNullException => visualizerTemplate = "Error message: " + ex.getMessage
      case ex: ColumnNullException => visualizerTemplate = "Error message: " + ex.getMessage
      case ex: InValidKeyException => visualizerTemplate = "Error message: " + ex.getMessage
    }

    if(visualizerTemplate.startsWith("Error message")){
      "<h3>" + visualizerTemplate + "</h3>"
    } else {
      visualizerTemplate
    }
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
  def generateHeadersRows(left: DataFrame, right: DataFrame, composite_key_strs: Seq[String] , maxRecords: Integer ) = {
    var vrt: VisualResultType = null
    var tempDf: DataFrame = left

    //if left is empty, visualResultType = VisualResultType.RIGHT
    //if right is empty, visualResultType = VisualResultType.LEFT
    //if neither is empty, visualResultType = VisualResultType.BOTH
    //if both are empty, no need to call visualize method???
    if(left.count() == 0) {
      vrt = VisualResultType.RIGHT;
      tempDf = right;
    } else if(right.count() == 0) {
      vrt = VisualResultType.LEFT;
      tempDf = left;
    } else {
      vrt = VisualResultType.BOTH;
    }

    val composite_keys_col = composite_key_strs.map(c => col(c)).toList
    val non_key_cols = tempDf.columns.filter(c => !composite_key_strs.contains(c)).toList
    var joinedRdd:DataFrame = null

    try{
      joinedRdd = left.as("l")
        .join(right.as("r"), composite_key_strs, "full_outer")
        .select(composite_keys_col:::non_key_cols.map(mapHelper):_*)
    } catch {
      case ex: SparkSessionNullException => throw new SparkSessionNullException(ex.getMessage);
      case ex: Exception => throw new ColumnNullException(ex.getMessage)
    }

    //get all rows from DataFrame
    val data: Array[Row] = joinedRdd.take(maxRecords)

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
  def mapHelper(value: String): Column = {
    val x = SparkFactory.sparkSession

    require(x != null, throw new SparkSessionNullException("Spark Session is null"))

    import x.implicits._

    //"<==>" is used as delimiter for mismatched left value and right value
    when($"l.$value".isNull && $"r.$value".isNull, lit("[null]"))
      .when($"l.$value".isNull, concat(lit("[null]<==>"), $"r.$value"))
      .when($"r.$value".isNull, concat($"l.$value", lit("<==>[null]")))
      .when($"l.$value" === $"r.$value", $"l.$value")
      .otherwise(concat($"l.$value",lit("<==>"),$"r.$value"))
      .as(value)
  }

  /**
    * Check whether passed sequence of keys are valid
    * @param composite_key_strs
    * @return
    */
  def isValidKey(composite_key_strs: Seq[String]): Boolean = {
    var flag: Boolean = true;

    composite_key_strs.foreach(k => {
      if(k == null || k.isEmpty) {
        flag = false;
      }
    })

    flag;
  }
}
