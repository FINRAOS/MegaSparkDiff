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

package org.finra.msd.sparkcompare

import org.apache.commons.lang3.tuple.{ImmutablePair, Pair}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.finra.msd.containers.AppleTable
import org.finra.msd.enums.SourceType
import org.finra.msd.sparkfactory.SparkFactory
import java.nio.file.{Paths, Files, StandardOpenOption}
import java.nio.charset.{StandardCharsets}
import scala.collection.JavaConverters._

/**
  * Contains comparison related operations
  */
object SparkCompare {

  /**
    * Can compare two files whether S3 , HDFS , local file system,
    * For example, for HDFS, "hdfs://nn1home:8020/input/war-and-peace.txt"
    * For S3 location, "s3n://myBucket/myFile1.log"
    *
    * @param file1Location location of file1 representing Left values
    * @param file2Location location of file2 representing Right values
    * @return a Pair of RDDs the Left parameter has values in RDD1 and not in RDD2,
    *         the Right parameter has values in RDD2 but not in RDD1
    */
  def compareFiles(file1Location: String, file2Location: String): Pair[DataFrame, DataFrame] = {
    val left: DataFrame = SparkFactory.parallelizeTextFile(file1Location)
    val right: DataFrame = SparkFactory.parallelizeTextFile(file2Location)
    return compareFlatDataFrames(left , right)
  }

  /**
    * Compares two data sources and stores results locally
    *
    * @param file1Location path to source1 data
    * @param file2Location path to source2 data
    * @param outputDirectory path where the comparison results have to be redirected
    * @param singleFileOutput a boolean variable to denote the number of output files to be one or more than one
    */
  def compareFileSaveResults(file1Location: String, file2Location: String , outputDirectory: String ,
                             singleFileOutput: Boolean) : Unit= {
    val resultPair: Pair[DataFrame, DataFrame] = compareFiles(file1Location,file2Location)
    saveResultsToDisk(resultPair.getLeft , resultPair.getRight , outputDirectory , singleFileOutput)
  }

  /**
    * Performs comparison between two custom source data types that were created from the actual source data details
    *
    * @param left Custom table for source1
    * @param right Custom table for source2
    * @param outputDirectory path where the comparison results have to be redirected
    * @param singleFileOutput a boolean variable to denote the number of output files to be one or more than one
    */
  def compareAppleTablesSaveResults(left: AppleTable , right: AppleTable , outputDirectory: String , singleFileOutput: Boolean) :Unit = {
    val result: Pair[DataFrame, DataFrame] = compareAppleTables(left,right)
    saveResultsToDisk(result.getLeft , result.getRight , outputDirectory , singleFileOutput)
  }

  /**
    * Performs schema based comparison irrespective of source data types
    *
    * @param left Custom table for source1
    * @param right Custom table for source2
    * @return a pair of RDDs, the left parameter has values in RDD1 and not in RDD2,
    *         the right parameter has values in RDD2 but not in RDD1
    */
  def compareAppleTables(left: AppleTable, right: AppleTable) :Pair[DataFrame, DataFrame] = {
    //if both are HIVE or JDBC then its a schema compare
    if (left.sourceType == SourceType.HIVE || left.sourceType == SourceType.JDBC)
      {
        if (right.sourceType == SourceType.HIVE || right.sourceType == SourceType.JDBC)
          {
            //DO A SCHEMA COMPARE even if one is HIVE and the other is JDBC
            return compareSchemaDataFrames(left.dataFrame , left.tempViewName,right.dataFrame , right.tempViewName)
          }
      }

    //Means one of them is not a schema data source and will flatten one of them
    var flatLeft: DataFrame = left.dataFrame
    var flatright: DataFrame = right.dataFrame

    //if one of the inputs has no schema then will flatten it using the delimiter
    if (left.sourceType == SourceType.HIVE || left.sourceType == SourceType.JDBC)
      flatLeft = SparkFactory.flattenDataFrame(left.dataFrame , left.delimiter)

    if (right.sourceType == SourceType.HIVE || right.sourceType == SourceType.JDBC)
      flatright = SparkFactory.flattenDataFrame(right.dataFrame , right.delimiter)

    //COMPARE flatLeft to flatRight
    return compareFlatDataFrames(flatLeft , flatright)
  }

  /**
    * Compares two single-column DataFrames
    *
    * @param left a dataframe generated form custom table for source1
    * @param right a dataframe generated form custom table for source2
    * @return a pair of RDDs, the left parameter has values in RDD1 and not in RDD2,
    *         the right parameter has values in RDD2 but not in RDD1
    */
  private def compareFlatDataFrames(left: DataFrame , right: DataFrame) :Pair[DataFrame, DataFrame] = {
    val leftGrouped: DataFrame = left.groupBy("values").count()
    val rightGrouped: DataFrame = right.groupBy("values").count()

    val inLnotinR = leftGrouped.except(rightGrouped).toDF()
    val inRnotinL = rightGrouped.except(leftGrouped).toDF()

    val subtractResult = new ImmutablePair[DataFrame, DataFrame](inLnotinR, inRnotinL)
    return subtractResult
  }

  /**
    *
    * @param left dataframe containing source1 data
    * @param leftViewName temporary table name of source1
    * @param right dataframe containing source2 data
    * @param rightViewName temporary table name of source2
    * @return a pair of RDDs, the left parameter has values in RDD1 and not in RDD2,
    *         the right parameter has values in RDD2 but not in RDD1
    */
   def compareSchemaDataFrames(left: DataFrame , leftViewName: String
                              , right: DataFrame , rightViewName: String) :Pair[DataFrame, DataFrame] = {
    //make sure that column names match in both dataFrames
    if (!left.columns.sameElements(right.columns))
      {
        println("column names were different")
        throw new Exception("Column Names Did Not Match")
      }

    val leftCols = left.columns.mkString(",")
    val rightCols = right.columns.mkString(",")

    //group by all columns in both data frames
    val groupedLeft = left.sqlContext.sql("select " + leftCols + " , count(*) as recordRepeatCount from " +  leftViewName + " group by " + leftCols )
    val groupedRight = left.sqlContext.sql("select " + rightCols + " , count(*) as recordRepeatCount from " +  rightViewName + " group by " + rightCols )

    //do the except/subtract command
    val inLnotinR = groupedLeft.except(groupedRight).toDF()
    val inRnotinL = groupedRight.except(groupedLeft).toDF()

    return new ImmutablePair[DataFrame, DataFrame](inLnotinR, inRnotinL)
  }

  /**
    * Stores comparison results locally
    *
    * @param leftResult a dataframe which contains the values in RDD1 and not in RDD2
    * @param rightResult a dataframe which contains the values in RDD2 and not in RDD1
    * @param outputDirectory location where the comparison results are to be stored
    * @param singleFile a boolean variable to denote the number of output files to be one or more than one
    */
  private def saveResultsToDisk(leftResult: DataFrame , rightResult: DataFrame,
                                outputDirectory: String , singleFile: Boolean) :Unit =
  {
    var left: DataFrame = leftResult
    var right: DataFrame = rightResult


    if (singleFile)
    {
      left = leftResult.coalesce(1)
      right = rightResult.coalesce(1)
    }

    // Write the symmetric difference to their own output directories
    val header : Boolean = true
    left.write.format("com.databricks.spark.csv").option("header", header + "").option("delimiter","\t").mode("overwrite").save(outputDirectory + "/inLeftNotInRight")
    right.write.format("com.databricks.spark.csv").option("header", header + "").option("delimiter","\t").mode("overwrite").save(outputDirectory + "/inRightNotInLeft")

    /*val niceData1 = nicelyFormatDF(left)
    val niceData2 = nicelyFormatDF(right)

    // Nicely format each symmetric difference table
    /*    val fileName1 = getFileName(outputDirectory + "/inLeftNotInRight")
        val file1Data: String = read(outputDirectory + "/inLeftNotInRight/" + fileName1)
        val niceData1 = nicelyFormatFile(file1Data, header)

        val fileName2 = getFileName(outputDirectory + "/inRightNotInLeft")
        val file2Data = read(outputDirectory + "/inRightNotInLeft/" + fileName2)
        val niceData2 = nicelyFormatFile(file2Data, header)*/
    
    // Output the nicely formatted differences to a single diff report
    scala.tools.nsc.io.File(outputDirectory + "/merged_report.txt").delete()
    scala.tools.nsc.io.File(outputDirectory + "/merged_report.txt").createFile()

    if (niceData1.length != 0 || niceData2.length != 0) {
      scala.tools.nsc.io.File(outputDirectory + "/merged_report.txt").appendAll("Contents which are in left table but not in right :" + "\n\n")
      write(outputDirectory + "/merged_report.txt", niceData1)

      scala.tools.nsc.io.File(outputDirectory + "/merged_report.txt").appendAll("\n\nContents which are in right table but not in left :" + "\n\n")
      write(outputDirectory + "/merged_report.txt", niceData2)
    }
    else
      scala.tools.nsc.io.File(outputDirectory + "/merged_report.txt").appendAll("Both data sets match")*/
  }

  private def nicelyFormatDF(dataFrame: DataFrame): String = {
    
    var nice : String = ""
    if(dataFrame.collect().length == 0)
      return nice
    else {
      val maxCols = new Array[Int](dataFrame.first().length)
      dataFrame.collect.foreach { row =>
        val colLengths = row.toSeq.map(e => e.toString.length)
        for (i <- maxCols.indices)
          if (colLengths(i) > maxCols(i))
            maxCols(i) = colLengths(i)
      }

      var r = 0
      dataFrame.collect.foreach { row =>

        // Generate a nicely formatted row
        var f = 0
        var rowS = ""
        row.toSeq.foreach { field =>
          val pad = maxCols(f) - field.toString.length
          var padS = ""
          for (j <- 1 to pad) {
            padS += " "
          }
          rowS += "│ " + field.toString + padS + " "
          f += 1
        }
        rowS += "│"

        // Out the row along with any other nicely formatted borders
        val baseSpecial: String = rowS.substring(1, rowS.length() - 1).replaceAll("[^│]", "─")
        if (r == 0)
          nice += "┌" + baseSpecial.replaceAll("│", "┬") + "┐\n"
        nice += rowS + "\n" // Output row
        //      if (r == 0 && isHeaderRow)
        //        nice += "├" + baseSpecial.replaceAll("│","┼") + "┤\n"
        if (r == dataFrame.collect().length - 1)
          nice += "└" + baseSpecial.replaceAll("│", "┴") + "┘\n"
        r += 1
      }
    }
    return nice
  }

  /**
    * Formats the comparison results and converts them in human readable format
    *
    * @param fileContent content of comparison result
    * @param isHeaderRow checks if it has column names or not
    * @return Formatted comparison results
    */
  private def nicelyFormatFile(fileContent : String, isHeaderRow : Boolean): String = {
    // Set up 2D array that stores the file contents in tabular format
    val lineSplit : Array[String] = fileContent.split("\n")
    val table : Array[Array[String]] = new Array[Array[String]](lineSplit.length)
    for (i <- lineSplit.indices)
      table(i) = lineSplit(i).split("\t")

    // Pad table entries so that each column is of the same length
    for (j <- table(0).indices)
    {
      var maxColLength = 0
      for (i <- table.indices)
        if (table(i)(j).length() > maxColLength)
          maxColLength = table(i)(j).length()

      for (i <- table.indices)
      {
        val padAmount = maxColLength - table(i)(j).length()
        for (s <- 0 until padAmount)
          table(i)(j) += " "
        table(i)(j) = " " + table(i)(j) + " "
      }
    }

    // Output the data nicely now that it's spaced evenly
    var nice : String = ""
    for (i <- table.indices)
    {
      var row : String = ""
      for (j <- table(i).indices)
        row += "│" + table(i)(j)
      row += "│"

      // Special row for marking the top, header, and bottom of nice table output
      val baseSpecial : String = row.substring(1,row.length()-1).replaceAll("[^│]","─")
      if (i == 0)
        nice += "┌" + baseSpecial.replaceAll("│","┬") + "┐\n"
      nice += row + "\n" // Output row
      if (i == 0 && isHeaderRow)
        nice += "├" + baseSpecial.replaceAll("│","┼") + "┤\n"

      if (i == table.length - 1)
        nice += "└" + baseSpecial.replaceAll("│","┴") + "┘\n"
    }

    return nice
  }


  /**
    * This method gets file name as string
    *
    * @param path data given the folder in which it resides
    * @return the output file that contains symmetric difference
    */
  private def getFileName(path: String): String = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    var fName: String = ""
    val list = hdfs.listFiles(new Path(path), true)

    while(list.hasNext()) {
      val tmp = list.next().getPath.getName()
      if(tmp.contains(".csv"))
        fName = tmp
    }
    return fName
  }

  /**
    * Return a file's contents as a String
    *
    * @param filePath location of the file generated by spark
    * @return a string containing file data
    */
  private def read(filePath:String): String = {
    val data = Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8).asScala.mkString("", "\n", "")
    return data
  }

  /**
    * Appends a String to a file
    *
    * @param filePath location of the report file
    * @param contents comparison results
    * @return
    */
  private def write(filePath:String, contents:String) = {
    Files.write(Paths.get(filePath), contents.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
  }

}
