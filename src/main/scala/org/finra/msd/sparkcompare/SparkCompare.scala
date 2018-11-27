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
import org.apache.spark.sql.functions.col
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
    if (left.sourceType == SourceType.HIVE || left.sourceType == SourceType.JDBC) {
      if (right.sourceType == SourceType.HIVE || right.sourceType == SourceType.JDBC) {
        //DO A SCHEMA COMPARE even if one is HIVE and the other is JDBC
        return compareSchemaDataFrames(left.dataFrame, right.dataFrame)
      }
    }

    //Means one of them is not a schema data source and will flatten one of them
    var flatLeft: DataFrame = left.dataFrame
    var flatright: DataFrame = right.dataFrame

    //if one of the inputs has no schema then will flatten it using the delimiter
    if (left.sourceType == SourceType.HIVE || left.sourceType == SourceType.JDBC)
      flatLeft = SparkFactory.flattenDataFrame(left.dataFrame, left.delimiter)

    if (right.sourceType == SourceType.HIVE || right.sourceType == SourceType.JDBC)
      flatright = SparkFactory.flattenDataFrame(right.dataFrame, right.delimiter)

    //COMPARE flatLeft to flatRight
    return compareFlatDataFrames(flatLeft, flatright)
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
    //make sure that column names match in both dataFrames
    if (!left.columns.sameElements(right.columns)) {
      throw new Exception("Column Names Did Not Match")
    }

    val groupedLeft = left.groupBy(left.getColumnsSeq(): _*)
      .count()
      .withColumnRenamed("count", "recordRepeatCount")

    val groupedRight = right.groupBy(right.getColumnsSeq(): _*)
      .count()
      .withColumnRenamed("count", "recordRepeatCount")

    //do the except/subtract command
    val inLnotinR = groupedLeft.except(groupedRight).toDF()
    val inRnotinL = groupedRight.except(groupedLeft).toDF()

    return new DiffResult(inLnotinR, inRnotinL)
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

  /**
    * This method does a full outer join between the resulting left and right dataframes from the method
    * SparkCompare.compareSchemaDataFrames. It will return a single dataframe having the left columns prefixed with l_
    * and the right columns prefixed with r_. the Key columns will not have prefixed. The resulting dataframe will have
    * all l_ columns on the left, then the Key columns in the middle, then the r_ columns on the right.
    *
    * @param left             Dataframe having inLeftNotInRight resulting from SparkCompare.compareSchemaDataFrames
    * @param right            Dataframe having inRightNotInLeft resulting from SparkCompare.compareSchemaDataFrames
    * @param compositeKeyStrs a Sequence of Strings having the primary keys applicable for both dataframes
    * @return a dataframe having the resulting full outer join operation.
    */
  def fullOuterJoinDataFrames(left: DataFrame, right: DataFrame, compositeKeyStrs: Seq[String]): DataFrame = {

    //convert column names to uppercase
    val upperCaseLeft: DataFrame = left.toDF(left.columns.map(_.toUpperCase): _*)
    val upperCaseRight: DataFrame = right.toDF(right.columns.map(_.toUpperCase): _*)

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
