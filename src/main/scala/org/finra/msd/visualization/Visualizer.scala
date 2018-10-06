package org.finra.msd.visualization

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.finra.msd.controllers.TemplateController
import org.finra.msd.customExceptions._
import org.finra.msd.enums.VisualResultType
import org.finra.msd.sparkfactory.SparkFactory

import scala.collection.immutable

object Visualizer {


  val ss = SparkFactory.sparkSession

  /**
    * Generate template string which can be used by displayHTML() in databricks
    *
    * @param left
    * @param right
    * @param compositeKeyStrs : a sequence of strings used as keys to do full outer join. Case does not matter.
    * @param maxRecords
    * @return a template string
    */
  def generateVisualizerTemplate(left: DataFrame, right: DataFrame, compositeKeyStrs: Seq[String],
                                 maxRecords: Integer = 1000): String = {

    var visualizerTemplate: String = "really?"
    var maxRecordsCopy: Integer = 1000;

    try {
      require(left != null, throw new DataFrameNullException("Left dataframe is null"));
      require(right != null, throw new DataFrameNullException("Right dataframe is null"));
      require(compositeKeyStrs != null && !compositeKeyStrs.isEmpty, throw new JoinKeysNullException(
        "Please specify primary/composite key"));
      require(isValidKey(compositeKeyStrs), throw new InValidKeyException("One or more keys is empty or null"))

      //handler invalid maxRecords
      if (maxRecords > 0) {
        maxRecordsCopy = maxRecords;
      }

      //if both dataframes are empty, then no need to do full outer join
      if (left.count() == 0 && right.count() == 0) {
        visualizerTemplate = "No mismatches are found";
      } else {
        val headersRows: (Seq[String], Seq[Seq[String]], VisualResultType) =
          generateHeadersRows(left, right, compositeKeyStrs, maxRecordsCopy);

        val headers: Seq[String] = headersRows._1;
        val rows: Seq[Seq[String]] = headersRows._2;
        val visualResultType: VisualResultType = headersRows._3;

        visualizerTemplate =
          s"""
          <!DOCTYPE html>
            <head>
              <meta charset="utf-8">
              <style>
                body {
                  font: 15px Lato, sans-serif;
                  font-weight: lighter;
                  transform: scale(1) !important;
                }

                table, tr, th, td {
                  margin: 0 auto;
                  max-width: 95%;
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
          if (visualResultType == VisualResultType.LEFT) {
            rows.map(row =>
              s"<tr>${
                row.map(cell =>
                  "<td>" +
                    s"<span class='spanBlue'>${cell}</span>" +
                    "</td>"
                ).mkString
              }</tr>"
            ).mkString
          } else if (visualResultType == VisualResultType.RIGHT) {
            rows.map(row =>
              s"<tr>${
                row.map(cell =>
                  "<td>" +
                    s"<span class='spanRed'>${cell}</span>" +
                    "</td>"
                ).mkString
              }</tr>"
            ).mkString
          } else {
            rows.map(row =>
              s"<tr>${
                row.map(cell => {
                  if (cell.contains("<==>")) {
                    val leftRightVals: Array[String] = cell.split("<==>")

                    "<td>" +
                      s"<span class='spanBlue'>${if (cell.startsWith("<==>")) "(empty)" else leftRightVals(0)}</span></br>" +
                      s"<span class='spanRed'>${if (cell.endsWith("<==>")) "(empty)" else leftRightVals(1)}</span>" +
                      "</td>"
                  } else {
                    s"<td>${cell}</td>"
                  }
                }).mkString
              }</tr>"
            ).mkString
          }
        }
              </table>
            </body>
          </html>
        """
      }
    } catch {
      case ex: DataFrameNullException => visualizerTemplate = "Error message: " + ex.getMessage
      case ex: JoinKeysNullException => visualizerTemplate = "Error message: " + ex.getMessage
      case ex: SparkSessionNullException => visualizerTemplate = "Error message: " + ex.getMessage
      case ex: ColumnNullException => visualizerTemplate = "Error message: " + ex.getMessage
      case ex: InValidKeyException => visualizerTemplate = "Error message: " + ex.getMessage
    }

    if (visualizerTemplate.startsWith("Error message") || visualizerTemplate.startsWith("No mismatches")) {
      "<h3>" + visualizerTemplate + "</h3>"
    } else {
      visualizerTemplate
    }
  }

  /**
    * Generate header, rows and a flag indicating the following scenarios:
    * 1 - right is empty
    * 2 - left is empty
    * 3 - both are not empty
    *
    * @param left
    * @param right
    * @param compositeKeyStrs
    * @param maxRecords
    * @return a tuple containing header, rows and visualResultType
    */
  def generateHeadersRows(left: DataFrame, right: DataFrame, compositeKeyStrs: Seq[String], maxRecords: Integer)
  : (Seq[String], Seq[Seq[String]], VisualResultType) = {

    //convert column names to uppercase
    val upperCaseLeft: DataFrame = left.toDF(left.columns.map(_.toUpperCase): _*)
    val upperCaseRight: DataFrame = right.toDF(right.columns.map(_.toUpperCase): _*)

    var visualResultType: VisualResultType = null
    var tempDf: DataFrame = upperCaseLeft

    //if left is empty, visualResultType = VisualResultType.RIGHT
    //if right is empty, visualResultType = VisualResultType.LEFT
    //if neither is empty, visualResultType = VisualResultType.BOTH
    //if both are empty, no need to call visualize method???
    if (upperCaseLeft.count() == 0) {
      visualResultType = VisualResultType.RIGHT;
      tempDf = upperCaseRight;
    } else if (upperCaseRight.count() == 0) {
      visualResultType = VisualResultType.LEFT;
      tempDf = upperCaseLeft;
    } else {
      visualResultType = VisualResultType.BOTH;
    }

    val caseTransformedKeys: Seq[String] = compositeKeyStrs.map(k => k.toUpperCase)
    val compositeKeysCol: List[Column] = caseTransformedKeys.map(c => col(c)).toList
    val nonKeyCols: List[String] = tempDf.columns.filter(c => !caseTransformedKeys.contains(c)).toList

    var joinedDf: DataFrame = null
    var data: Array[Row] = null;

    if (visualResultType == VisualResultType.BOTH) {
      try {
        joinedDf = upperCaseLeft.as("l")
          .join(upperCaseRight.as("r"), caseTransformedKeys, "full_outer")
          .select(compositeKeysCol ::: nonKeyCols.map(mapHelper): _*)
      } catch {
        case ex: SparkSessionNullException => throw new SparkSessionNullException(ex.getMessage);
        case ex: Exception => throw new ColumnNullException(ex.getMessage)
      }
    }

    if (joinedDf == null) {
      joinedDf = tempDf;
    }

    //get all rows from DataFrame
    data = joinedDf.take(maxRecords)

    val headers: Seq[String] = joinedDf.schema.fieldNames.toSeq.map(header => header.toUpperCase)
    val rows: Seq[Seq[String]] = data.map { row =>
      row.toSeq.map { cell =>
        cell match {
          case null => "[null]"
          case _ => cell.toString
        }
      }: Seq[String]
    }

    (headers, rows, visualResultType)
  }

  /**
    * Helper method to map non-composite key cell value to desired value
    *
    * @param columnName : column name
    * @return
    */
  def mapHelper(columnName: String): Column = {

    val x: SparkSession = SparkFactory.sparkSession

    require(x != null, throw new SparkSessionNullException("Spark Session is null"))

    import x.implicits._

    //"<==>" is used as delimiter for mismatched left value and right value
    when($"l.$columnName".isNull && $"r.$columnName".isNull, lit("[null]"))
      .when($"l.$columnName".isNull, concat(lit("[null]<==>"), $"r.$columnName"))
      .when($"r.$columnName".isNull, concat($"l.$columnName", lit("<==>[null]")))
      .when($"l.$columnName" === $"r.$columnName", concat(lit(""), $"l.$columnName"))
      .otherwise(concat($"l.$columnName", lit("<==>"), $"r.$columnName"))
      .as(columnName)
  }

  /**
    * Check whether passed sequence of keys are valid
    *
    * @param composite_key_strs
    * @return
    */
  def isValidKey(composite_key_strs: Seq[String]): Boolean = {

    var flag: Boolean = true;

    composite_key_strs.foreach(key => {
      if (key == null || key.isEmpty) {
        flag = false;
      }
    })

    flag;
  }

  /**
    *
    * @param df the resulting dataframe from FULL OUTER JOIN operation by SparkCompare.fullOuterJoinDataFrames
    * @param limit the maximum number of records to be displayed in the table
    * @return HTML table as a String to be further used as placement inside the horizontalTemplate
    */
  def renderHorizontalTable(df: DataFrame, limit: Int): String = {
    val rows = df.take(limit).toSeq
    val header: Seq[String] = df.schema.fieldNames.toSeq
    val htmlTableWithNoHeader = rows.map(row => s"""<tr>${convertRowToHtml(row, header)}</tr>${System.lineSeparator()}""").mkString

    val headerHtml = "<tr>" + header.map(h => "<th>" + h + "</th>").mkString + "</tr>" + System.lineSeparator()

    val htmlTable = "<table>" + headerHtml + htmlTableWithNoHeader + "</table>"

    val html = TemplateController.horizontalTableTemplate.replace("#tableContent" , htmlTable)

    html
  }

  /**
    *
    * @param row a single row of the full outer join dataframe created by SparkCompare.fullOuterJoinDataFrames
    * @param header a sequence of strings having the header column names. expectation is that it is ordered like so
    *               l_column1 l_column2 key1 key2 r_column1 r_column2
    * @return a string having HTML representation of a single htmlt able row
    */
  private def convertRowToHtml(row: Row, header: Seq[String]): String = {
    val valuesMap: Map[String, Nothing] = row.getValuesMap(header)
    header.map(h => getValueFromRowAsCell(valuesMap, h)).mkString
  }

  /**
    *
    * @param valuesMap a key value map extracted from the dataframe ROW object
    * @param columnName the column name for which the caller wants to extract the value and render as html TD
    * @return html TD encapsulating the column value. If the value between left and right are different then it will
    *         have CSS class of "different" if the values are the same then the css class will have value of "same".
    *         if the column is a key column it will have a css class of "same"
    */
  private def getValueFromRowAsCell(valuesMap: Map[String, Nothing], columnName: String): String = {
    val value = {
      val valueOption = valuesMap.get(columnName)
      if (valueOption.get == null) "null"
      else valueOption.get.toString
    }

    //here we will get the value of l_column or r_column depending on what we got as input
    val valueOther = {
      val otherColumnName = {
        if (columnName.startsWith("l_")) {
          Option.apply(columnName.replace("l_", "r_"))
        } else if (columnName.startsWith("r_")) {
          Option.apply(columnName.replace("r_", "l_"))
        } else {
          Option.empty
        }
      }

      if (otherColumnName.isEmpty) Option.empty //this means its a key column
      else //this means its a value column
      {
        val otherColumnValue = valuesMap.get(otherColumnName.get)
        if (otherColumnValue.get == null) Option.apply("null")
        else Option.apply(otherColumnValue.get.toString)
      }
    }

    val htmlDiffClass = {
      if (valueOther.isEmpty) "same"
      else if (value.equals(valueOther.get)) "same"
      else "different"
    }

    s"""<td class='${htmlDiffClass}'>${value}</td>"""
  }
}
