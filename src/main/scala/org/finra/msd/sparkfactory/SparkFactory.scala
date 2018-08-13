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

package org.finra.msd.sparkfactory

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.finra.msd.containers.AppleTable
import org.finra.msd.enums.SourceType

object SparkFactory {


  var sparkSession: SparkSession = null

  var conf: SparkConf = null

  /**
    * The initialize method creates the main spark session which MegaSparkDiff uses.
    * This needs to be called before any operation can be made using MegaSparkDiff.
    * It creates a spark app with the name "megasparkdiff" and enables hive support by default.
    */
  def initializeSparkContext(): Unit = { //todo: need to refactor it to support spark-submit command
      conf = new SparkConf().setAppName("megasparkdiff")
      sparkSession = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  }


  /**
    * This method should be used to initialize MegaSparkDiff in local mode, in other words anything that is not EMR
    * or EC2. The typical use cases are if you are executing a diff on your laptop or workstation or within a Jenkins
    * build.
    *
    * @param numCores this parameters can be used to set the number of cores you wanna specify for spark. for example
    *                 you can specify "local[1]" this means spark will use 1 core only. Alternatively you can specify
    *                 "local[*]" this means spark will figure out how many cores you have and will use them all.
    */
  def initializeSparkLocalMode(numCores: String , logLevel :String , defaultPartitions :String): Unit = {
    conf = new SparkConf().setAppName("megasparkdiff")
      .setMaster(numCores)
      .set("spark.driver.host", "localhost")
      .set("spark.ui.enabled","false") //disable spark UI
      .set("spark.sql.shuffle.partitions",defaultPartitions)
    sparkSession = SparkSession.builder.config(conf).getOrCreate()
    sparkSession.sparkContext.setLogLevel(logLevel)
  }

  def initializeDataBricks(dataBricksSparkSession: SparkSession) : Unit ={
    sparkSession = dataBricksSparkSession;
  }
  /**
    * Terminates the current Spark session
    */
  def stopSparkContext(): Unit = {
    sparkSession.stop()
  }

  /**
    * This method will create DataFrame with a single column called "field1" from a text file, the file can be on local
    * machine or HDFS for HDFS url like so
    * "hdfs://nn1home:8020/input/war-and-peace.txt" for S3 the url like so
    * "s3n://myBucket/myFile1.log"
    *
    * @param textFileLocation path of a flat file containing the data to be compared
    * @return a dataframe converted from a flat file
    */
  def parallelizeTextFile(textFileLocation: String): DataFrame = {
    //parallelize a text file and map each line to an untyped Row object hence getting an RDD of type Row
    val rowRDD: RDD[Row] = sparkSession.sparkContext.textFile(textFileLocation).map((row: String) => RowFactory.create(row))
    // Creates a schema of a single column called Values of type String
    val schema: StructType = DataTypes.createStructType(Array[StructField](DataTypes.createStructField("values", DataTypes.StringType, true)))
    //Ask spark SQL to create a dataFrame based on the RDD[Row]
    val df: DataFrame = sparkSession.sqlContext.createDataFrame(rowRDD, schema).toDF
    return df
  }

  /**
    * This method will create DataFrame with a single column called "field1" from a text file, the file can be on local
    * machine or HDFS for HDFS url like so
    * "hdfs://nn1home:8020/input/war-and-peace.txt" for S3 the url like so
    * "s3n://myBucket/myFile1.log"
    * @param textFileLocation path of a flat file containing the data to be compared
    * @param tempViewName temporary table name for source data
    * @return custom table containing the data to be compared
    */
  def parallelizeTextSource(textFileLocation: String, tempViewName: String): AppleTable = {
    val df = parallelizeTextFile(textFileLocation)
    val a: AppleTable = new AppleTable(SourceType.FILE , df , null , tempViewName )
    return a
  }

  /**
    * This method will create an AppleTable from a query that retrieves data from a database
    * accessed through JDBC connection.
    *
    * @param driverClassName JDBC driver name
    * @param jdbcUrl JDBC URL
    * @param username Username for database connection
    * @param password Password for database connection
    * @param sqlQuery Query to retrieve the desired data from database
    * @param tempViewName temporary table name for source data
    * @param delimiter source data separation character
    * @return custom table containing the data to be compared
    */
  def parallelizeJDBCSource(driverClassName: String, jdbcUrl: String, username: String, password: String, sqlQuery: String,
                          tempViewName: String , delimiter: Option[String] ) : AppleTable =
  {
    val jdbcDF: DataFrame = sparkSession.sqlContext.read
      .format("jdbc")
      .option("driver" , driverClassName)
      .option("url", jdbcUrl)
      .option("dbtable", sqlQuery)
      .option("user", username)
      .option("password", password)
      .load()
    jdbcDF.createOrReplaceTempView(tempViewName)

    val appleTable: AppleTable = new AppleTable(SourceType.JDBC,  jdbcDF,delimiter.getOrElse(null) , tempViewName)
    return appleTable
  }

  /**
    * This method will create an AppleTable from a query that retrieves data from a database
    * accessed through JDBC connection.
    *
    * @param driverClassName JDBC driver name
    * @param jdbcUrl JDBC URL
    * @param username Username for database connection
    * @param password Password for database connection
    * @param sqlQuery Query to retrieve the desired data from database
    * @param tempViewName temporary table name for source data
    * @param delimiter source data separation character
    * @param partitionColumn numeric column on which the table should be partitioned on
    * @param lowerBound typically the minimum of the partition column
    * @param upperBound typically the maximum of the partition column
    * @param numPartitions number of partitions to break the table into, typically number of worker cores on cluster
    * @param fetchSize number of rows to fetch per network request, default is at a low 10
    * @return custom table containing the data to be compared
    */
  def parallelizeJDBCSource(driverClassName: String, jdbcUrl: String, username: String, password: String, sqlQuery: String,
                            tempViewName: String , delimiter: Option[String] , partitionColumn: String
                           , lowerBound :String , upperBound :String, numPartitions :String, fetchSize: String) : AppleTable =
  {
    val jdbcDF: DataFrame = sparkSession.sqlContext.read
      .format("jdbc")
      .option("driver" , driverClassName)
      .option("url", jdbcUrl)
      .option("dbtable", sqlQuery)
      .option("user", username)
      .option("password", password)
      .option("partitionColumn", partitionColumn)
      .option("lowerBound", lowerBound)
      .option("upperBound", upperBound)
      .option("numPartitions", numPartitions)
      .option("fetchsize", fetchSize)
      .load()
    jdbcDF.createOrReplaceTempView(tempViewName)

    val appleTable: AppleTable = new AppleTable(SourceType.JDBC,  jdbcDF,delimiter.getOrElse(null) , tempViewName)
    return appleTable
  }

  /**
    *This method will create an AppleTable from a query that retrieves data from a database
    * accessed through JDBC connection; passes in "," as a default delimiter
    *
    * @param driverClassName JDBC driver name
    * @param jdbcUrl JDBC URL
    * @param username Username for database connection
    * @param password Password for database connection
    * @param sqlQuery Query to retrieve the desired data from database
    * @param tempViewName temporary table name for source data
    * @return
    */
  def parallelizeJDBCSource(driverClassName: String, jdbcUrl: String, username: String, password: String, sqlQuery: String,
                          tempViewName: String ) : AppleTable = {
    parallelizeJDBCSource(driverClassName,jdbcUrl,username,password,sqlQuery,tempViewName,Option.apply(","))
  }

  /**
    * Flattens a DataFrame to only have a single column that contains the entire original row
    * @param df a dataframe which contains one or more columns
    * @param delimiter a character which separates the source data
    * @return flattened dataframe
    */
  def flattenDataFrame(df: DataFrame , delimiter: String) :DataFrame = {
    //There is a bug open for this in the Spark Bug tracker
    val x = SparkFactory.sparkSession
    import x.implicits._

    val flatLeft = df.map(row => row.mkString(delimiter)).toDF("values")
    return flatLeft
  }

  /**
    * This method will create an AppleTable from a query that retrieves data from a hive
    * table.  It is assumed that hive connectivity is already enabled in the environment from
    * which this project is run.
    * @param sqlText a query to retrieve the data
    * @param tempViewName custom table name for source
    * @return custom table containing the data
    */
  def parallelizeHiveSource(sqlText: String , tempViewName: String) : AppleTable = {
    val df: DataFrame = sparkSession.sql(sqlText)
    df.createOrReplaceTempView(tempViewName)
    val  a:AppleTable = new AppleTable(SourceType.HIVE , df , "," ,tempViewName )
    return a
  }

}

