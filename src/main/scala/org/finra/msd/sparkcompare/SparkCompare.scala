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

import org.apache.spark.sql._
import org.finra.msd.containers.{AppleTable, CountResult, DiffResult}
import org.finra.msd.enums.SourceType
import org.finra.msd.implicits.DataFrameImplicits._
import org.finra.msd.outputwriters.OutputWriter
import org.finra.msd.sparkfactory.SparkFactory

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
  def compareFiles(file1Location: String, file2Location: String): DiffResult = {
    val left: DataFrame = SparkFactory.parallelizeTextFile(file1Location)
    val right: DataFrame = SparkFactory.parallelizeTextFile(file2Location)
    return compareFlatDataFrames(left, right)
  }

  /**
    * Compares two data sources and stores results locally
    *
    * @param file1Location    path to source1 data
    * @param file2Location    path to source2 data
    * @param outputDirectory  path where the comparison results have to be redirected
    * @param singleFileOutput a boolean variable to denote the number of output files to be one or more than one
    */
  def compareFileSaveResults(file1Location: String, file2Location: String, outputDirectory: String,
                             singleFileOutput: Boolean, delimiter: String): Unit = {
    val resultPair: DiffResult = compareFiles(file1Location, file2Location)
    OutputWriter.saveResultsToDisk(resultPair.inRightNotInLeft, resultPair.inRightNotInLeft, outputDirectory, singleFileOutput, delimiter)
  }

  /**
    * Performs comparison between two custom source data types that were created from the actual source data details
    *
    * @param left             Custom table for source1
    * @param right            Custom table for source2
    * @param outputDirectory  path where the comparison results have to be redirected
    * @param singleFileOutput a boolean variable to denote the number of output files to be one or more than one
    */
  def compareAppleTablesSaveResults(left: AppleTable, right: AppleTable, outputDirectory: String, singleFileOutput: Boolean, delimiter: String): Unit = {
    val result: DiffResult = compareAppleTables(left, right)
    OutputWriter.saveResultsToDisk(result.inLeftNotInRight, result.inRightNotInLeft, outputDirectory, singleFileOutput, delimiter)
  }

  def compareAppleTablesSaveResultsWithManipulation(left: AppleTable, right: AppleTable,
                                                    outputDirectory: String, singleFileOutput: Boolean, delimiter: String,
                                                    excludeCols: Option[Array[String]],
                                                    orderByCols: Option[Array[String]], ascOrder: Boolean): Boolean = {
    var result: DiffResult = compareAppleTables(left, right)
    if(excludeCols.getOrElse(0) != 0) {
      result = result.removeCols(excludeCols.get)
    }

    if(orderByCols.getOrElse(0) != 0) {
      result = result.getOrderedResult(orderByCols.get, isAsc = ascOrder)
    }

    OutputWriter.saveResultsToDisk(result.inLeftNotInRight, result.inRightNotInLeft, outputDirectory, singleFileOutput, delimiter)
    result.noDiff()

  }

  /**
    * Performs schema based comparison irrespective of source data types
    *
    * @param left  Custom table for source1
    * @param right Custom table for source2
    * @return a pair of DataFrames, the left parameter has values in DF1 and not in DF2,
    *         the right parameter has values in DF2 but not in DF1
    */
  def compareAppleTables(left: AppleTable, right: AppleTable): DiffResult = {
    //if both are HIVE or JDBC then its a schema compare
    if (left.sourceType != SourceType.FILE && right.sourceType != SourceType.FILE) {
      var expandLeft = left.dataFrame
      var expandRight = right.dataFrame

      val leftCol = expandLeft.columns
      val rightCol = expandRight.columns

      if ((left.sourceType == SourceType.DYNAMODB || left.sourceType == SourceType.JSON)
        && (right.sourceType == SourceType.DYNAMODB || right.sourceType == SourceType.JSON)) {
        // Both tables are JSON format
        val (expandLeftNew, expandRightNew) = SparkFactory.syncJSONFormatSchemaDataFrame(expandLeft, expandRight)

        expandLeft = expandLeftNew
        expandRight = expandRightNew
      }
      else {
        //make sure that column names match in both dataFrames
        if (!expandLeft.columns.sameElements(expandRight.columns)) {
          throw new Exception("Column Names Did Not Match")
        }

        if (left.sourceType == SourceType.DYNAMODB || left.sourceType == SourceType.JSON) {
          // Left table is JSON format and right table is not
          expandLeft = SparkFactory.complexJSONFormatTableToSimpleJSONFormatTable(expandLeft)
          expandRight = SparkFactory.simpleTableToSimpleJSONFormatTable(expandRight)

          val (expandLeftNew, expandRightNew) = SparkFactory.syncJSONFormatSchemaDataFrame(expandLeft, expandRight)

          expandLeft = expandLeftNew
          expandRight = expandRightNew
        }
        else if (right.sourceType == SourceType.DYNAMODB || right.sourceType == SourceType.JSON) {
          // Right table is JSON format and left table is not
          expandRight = SparkFactory.complexJSONFormatTableToSimpleJSONFormatTable(expandRight)
          expandLeft = SparkFactory.simpleTableToSimpleJSONFormatTable(expandLeft)

          val (expandLeftNew, expandRightNew) = SparkFactory.syncJSONFormatSchemaDataFrame(expandLeft, expandRight)

          expandLeft = expandLeftNew
          expandRight = expandRightNew
        }
      }

      //DO A SCHEMA COMPARE even if one is HIVE and the other is JDBC, or JSON format transformations have been applied
      return compareSchemaDataFrames(expandLeft, expandRight)
    }

    //Means one of them is not a schema data source and will flatten one of them
    var flatLeft: DataFrame = left.dataFrame
    var flatRight: DataFrame = right.dataFrame

    //if one of the inputs has no schema then will flatten it using the delimiter
    if (left.sourceType == SourceType.HIVE || left.sourceType == SourceType.JDBC)
      flatLeft = SparkFactory.flattenDataFrame(left.dataFrame, left.delimiter)

    if (left.sourceType == SourceType.DYNAMODB || left.sourceType == SourceType.JSON)
      flatLeft = SparkFactory.removeComplexNullFromFlatDataFrame(
        SparkFactory.flattenDataFrame(SparkFactory.complexJSONFormatTableToSimpleJSONFormatTable(flatLeft), left.delimiter))

    if (right.sourceType == SourceType.HIVE || right.sourceType == SourceType.JDBC)
      flatRight = SparkFactory.flattenDataFrame(right.dataFrame, right.delimiter)

    if (right.sourceType == SourceType.DYNAMODB || right.sourceType == SourceType.JSON)
      flatRight = SparkFactory.removeComplexNullFromFlatDataFrame(
        SparkFactory.flattenDataFrame(SparkFactory.complexJSONFormatTableToSimpleJSONFormatTable(flatRight), right.delimiter))

    //COMPARE flatLeft to flatRight
    return compareFlatDataFrames(flatLeft, flatRight)
  }

  /**
    * Compares two single-column DataFrames
    *
    * @param left  a dataframe generated form custom table for source1
    * @param right a dataframe generated form custom table for source2
    * @return a pair of RDDs, the left parameter has values in RDD1 and not in RDD2,
    *         the right parameter has values in RDD2 but not in RDD1
    */
  private def compareFlatDataFrames(left: DataFrame, right: DataFrame): DiffResult = {
    val leftGrouped: DataFrame = left.groupBy("values").count()
    val rightGrouped: DataFrame = right.groupBy("values").count()

    val inLnotinR = leftGrouped.except(rightGrouped).toDF()
    val inRnotinL = rightGrouped.except(leftGrouped).toDF()

    val subtractResult = new DiffResult(inLnotinR, inRnotinL)
    return subtractResult
  }

  /**
    *
    * @param left          dataframe containing source1 data
    * @param right         dataframe containing source2 data
    * @return a pair of RDDs, the left parameter has values in RDD1 and not in RDD2,
    *         the right parameter has values in RDD2 but not in RDD1
    */
  def compareSchemaDataFrames(left: DataFrame, right: DataFrame): DiffResult = {
    val groupedLeft = left.groupBy(left.getColumnsSeq(): _*)
      .count()
      .withColumnRenamed("count", "recordRepeatCount")

    val groupedRight = right.groupBy(right.getColumnsSeq(): _*)
      .count()
      .withColumnRenamed("count", "recordRepeatCount")

    //do the except/subtract command
    val inLnotinR = groupedLeft.except(groupedRight).toDF()
    val inRnotinL = groupedRight.except(groupedLeft).toDF()

    return DiffResult(inLnotinR, inRnotinL)
  }

  /**
    * Performs schema based comparison irrespective of source data types
    *
    * @param left  Custom table for LeftSource
    * @param right Custom table for RightSource
    * @return a pair containing the count in left and right
    */
  def compareAppleTablesCount(left: AppleTable, right: AppleTable): CountResult = {

    val leftCount = left.dataFrame.count()
    val rightCount = right.dataFrame.count()

    val countsPair = new CountResult(leftCount, rightCount)

    return countsPair
  }
}
