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
    * @param compositeKeyStrs
    * @return a template string
    */
  def generateVisualizerTemplate(left: DataFrame, right: DataFrame, compositeKeyStrs: Seq[String] , maxRecords: Integer = 1000): String = {
    var visualizerTemplate: String = "really?"

    try{
      require(left != null, throw new DataFrameNullException("Left table is null"));
      require(right != null, throw new DataFrameNullException("Right table is null"));
      require(compositeKeyStrs != null && !compositeKeyStrs.isEmpty, throw new JoinKeysNullException(
        "Please specify primary/composite key"));
      require(isValidKey(compositeKeyStrs), throw new InValidKeyException("One or more keys is empty or null"))

      val headersRows = generateHeadersRows(left, right, compositeKeyStrs, maxRecords);

      val headers = headersRows._1;
      val rows = headersRows._2;
      val visualResultType = headersRows._3;

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
            text-align: center;
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
            color: red;
          }
        </style>
      </head>
      <body>
        <table>
          <tr>
            ${headers.map(header => s"<th>${header}</th>").mkString}
          </tr>
          ${
          if(visualResultType == VisualResultType.LEFT) {
            rows.map(row =>
              s"<tr>${row.map(cell =>
                if(cell.contains("<==>")) {
                  val leftRightVals: Array[String] = cell.split("<==>")

                  "<td>" +
                    s"<span class='spanBlue'>${leftRightVals(0)}</span>" +
                    "</td>"
                } else {
                  "<td>" +
                    s"<span class='spanBlue'>${cell}</span>" +
                    "</td>"
                }
              ).mkString}</tr>"
            ).mkString
          } else if(visualResultType == VisualResultType.RIGHT) {
            rows.map(row =>
              s"<tr>${row.map(cell =>
                if(cell.contains("<==>")) {
                  val leftRightVals: Array[String] = cell.split("<==>")

                  "<td>" +
                    s"<span class='spanRed'>${leftRightVals(1)}</span>" +
                    "</td>"
                } else {
                  "<td>" +
                    s"<span class='spanRed'>${cell}</span>" +
                    "</td>"
                }
              ).mkString}</tr>"
            ).mkString
          } else{
            rows.map(row =>
              s"<tr>${row.map (cell => {
                if(cell.contains("<==>")){
                  val leftRightVals: Array[String] = cell.split("<==>")

                  "<td>" +
                    s"<span class='spanBlue'>${if(leftRightVals(0) == "") "(empty)" else leftRightVals(0)}</span></br>" +
                    s"<span class='spanRed'>${if(leftRightVals(1) == "") "(empty)" else leftRightVals(1)}</span>" +
                    "</td>"
                } else {
                  s"<td>${cell}</td>"
                }
              }).mkString}</tr>"
            ).mkString
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
    * @param compositeKeyStrs: a sequence of columns to make up a composite key
    * @return a tuple containing header, rows and visualResultType
    */
  def generateHeadersRows(left: DataFrame, right: DataFrame, compositeKeyStrs: Seq[String] , maxRecords: Integer ) = {
    //convert column names to uppercase
    val upperCaseLeft = left.toDF(left.columns.map(_.toUpperCase): _*)
    val upperCaseRight = right.toDF(right.columns.map(_.toUpperCase): _*)

    var visualResultType: VisualResultType = null
    var tempDf: DataFrame = upperCaseLeft

    //if left is empty, visualResultType = VisualResultType.RIGHT
    //if right is empty, visualResultType = VisualResultType.LEFT
    //if neither is empty, visualResultType = VisualResultType.BOTH
    //if both are empty, no need to call visualize method???
    if(upperCaseLeft.count() == 0) {
      visualResultType = VisualResultType.RIGHT;
      tempDf = upperCaseRight;
    } else if(upperCaseRight.count() == 0) {
      visualResultType = VisualResultType.LEFT;
      tempDf = upperCaseLeft;
    } else {
      visualResultType = VisualResultType.BOTH;
    }

    val caseTransformedKeys = compositeKeyStrs.map(k => k.toUpperCase)
    val compositeKeysCol = caseTransformedKeys.map(c => col(c)).toList
    val nonKeyCols = tempDf.columns.filter(c => !caseTransformedKeys.contains(c)).toList
    var joinedDf:DataFrame = null

    try{
      joinedDf = upperCaseLeft.as("l")
        .join(upperCaseRight.as("r"), caseTransformedKeys, "full_outer")
        .select(compositeKeysCol:::nonKeyCols.map(mapHelper):_*)
    } catch {
      case ex: SparkSessionNullException => throw new SparkSessionNullException(ex.getMessage);
      case ex: Exception => throw new ColumnNullException(ex.getMessage)
    }

    //get all rows from DataFrame
    val data: Array[Row] = joinedDf.take(maxRecords)

    val headers: Seq[String] = joinedDf.schema.fieldNames.toSeq.map(header => header.toUpperCase)
    val rows: Seq[Seq[String]] = data.map {row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case _ => cell.toString
        }
        str
      }: Seq[String]
    }

    (headers, rows, visualResultType)
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

    composite_key_strs.foreach(key => {
      if(key == null || key.isEmpty) {
        flag = false;
      }
    })

    flag;
  }
}
