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

import com.google.gson.Gson
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.finra.msd.containers.AppleTable
import org.finra.msd.enums.SourceType

import scala.collection.mutable

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
  def initializeSparkLocalMode(numCores: String, logLevel: String, defaultPartitions: String): Unit = synchronized {
    if (sparkSession == null)
      {
        conf = new SparkConf().setAppName("megasparkdiff")
          .setMaster(numCores)
          .set("spark.driver.host", "localhost")
          .set("spark.ui.enabled", "false") //disable spark UI
          .set("spark.sql.shuffle.partitions", defaultPartitions)
        sparkSession = SparkSession.builder.config(conf).getOrCreate()
        sparkSession.sparkContext.setLogLevel(logLevel)
      }
  }

  def initializeDataBricks(dataBricksSparkSession: SparkSession): Unit = {
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
    *
    * @param textFileLocation path of a flat file containing the data to be compared
    * @param tempViewName     temporary table name for source data
    * @return custom table containing the data to be compared
    */
  def parallelizeTextSource(textFileLocation: String, tempViewName: String): AppleTable = {
    val df = parallelizeTextFile(textFileLocation)
    val a: AppleTable = new AppleTable(SourceType.FILE, df, null, tempViewName)
    return a
  }

  /** This method will create an AppleTable for data in a JSON file.
    *
    * @param jsonFileLocation path of a json file containing the data to be compared
    * @param tempViewName     temporary table name for source data
    * @param delimiter        source data separation character
    * @return custom table containing the data to be compared
    */
  def parallelizeJSONSource(jsonFileLocation: String, tempViewName: String, delimiter: Option[String]): AppleTable = {
    val df = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .json(jsonFileLocation)
    df.createOrReplaceTempView(tempViewName)

    AppleTable(SourceType.JSON, df, delimiter.orNull, tempViewName)
  }

  /** This method will create an AppleTable for data in a JSON file.
    *
    * @param jsonFileLocation path of a json file containing the data to be compared
    * @param tempViewName     temporary table name for source data
    * @return custom table containing the data to be compared
    */
  def parallelizeJSONSource(jsonFileLocation: String, tempViewName: String): AppleTable = {
    parallelizeJSONSource(jsonFileLocation, tempViewName, Option.apply(","))
  }

  /**
    * Create DataFrame from a delimited text file.
    * The file can be on local machine or HDFS or other file system supported by the Spark implementation.
    * "hdfs://nn1home:8020/input/war-and-peace.txt" for S3 the url like so
    * "s3n://myBucket/myFile1.log"
    *
    * @param fileLocation path of file
    * @param delimiter delimiter used in the delimited file
    * @return DataFrame created from the file
    */
  def parallelizeDelimitedFile(fileLocation: String, delimiter: String = ","): DataFrame = {
    sparkSession.read
      .option("delimiter", delimiter)
      .csv(fileLocation)
  }

  /**
    * Create AppleTable from a delimited text file.
    * The file can be on local machine or HDFS or other file system supported by the Spark implementation.
    * "hdfs://nn1home:8020/input/war-and-peace.txt" for S3 the url like so
    * "s3n://myBucket/myFile1.log"
    *
    * @param fileLocation path of file
    * @param delimiter delimiter used in the delimited file
    * @param tempViewName    temporary table name for source data
    * @return AppleTable created from the file
    */
  def parallelizeDelimitedSource(fileLocation: String, tempViewName: String, delimiter: String = ","): AppleTable = {
    val df = parallelizeDelimitedFile(fileLocation, delimiter)
    df.createOrReplaceTempView(tempViewName)
    new AppleTable(SourceType.CSV, df, delimiter, tempViewName)
  }

  /**
    * Create DataFrame from a delimited text file, applying the specified schema.
    * The file can be on local machine or HDFS or other file system supported by the Spark implementation.
    * "hdfs://nn1home:8020/input/war-and-peace.txt" for S3 the url like so
    * "s3n://myBucket/myFile1.log"
    *
    * @param fileLocation path of file
    * @param delimiter delimiter used in the delimited file
    * @param ddl schema specified in DDL style, e.g. "name VARCHAR(50), age INT"
    * @return DataFrame created from the file
    */
  def parallelizeDelimitedFileWithDdl(fileLocation: String, ddl: String, delimiter: String = ","): DataFrame = {
    sparkSession.read
      .option("delimiter", delimiter)
      .schema(ddl)
      .csv(fileLocation)
  }

  /**
    * Create AppleTable from a delimited text file.
    * The file can be on local machine or HDFS or other file system supported by the Spark implementation.
    * "hdfs://nn1home:8020/input/war-and-peace.txt" for S3 the url like so
    * "s3n://myBucket/myFile1.log"
    *
    * @param fileLocation path of file
    * @param delimiter delimiter used in the delimited file
    * @param tempViewName    temporary table name for source data
    * @return AppleTable created from the file
    */
  def parallelizeDelimitedSourceWithDdl(fileLocation: String, ddl: String, tempViewName: String, delimiter: String = ","): AppleTable = {
    val df = parallelizeDelimitedFileWithDdl(fileLocation, ddl, delimiter)
    df.createOrReplaceTempView(tempViewName)
    new AppleTable(SourceType.CSV, df, delimiter, tempViewName)
  }


  /**
    * This method will create an AppleTable from a query that retrieves data from a database
    * accessed through JDBC connection.
    *
    * @param driverClassName JDBC driver name
    * @param jdbcUrl         JDBC URL
    * @param username        Username for database connection
    * @param password        Password for database connection
    * @param sqlQuery        Query to retrieve the desired data from database
    * @param tempViewName    temporary table name for source data
    * @param delimiter       source data separation character
    * @return custom table containing the data to be compared
    */
  def parallelizeJDBCSource(driverClassName: String, jdbcUrl: String, username: String, password: String, sqlQuery: String,
                            tempViewName: String, delimiter: Option[String]): AppleTable = {
    val jdbcDF: DataFrame = sparkSession.sqlContext.read
      .format("jdbc")
      .option("driver", driverClassName)
      .option("url", jdbcUrl)
      .option("dbtable", sqlQuery)
      .option("user", username)
      .option("password", password)
      .load()
    jdbcDF.createOrReplaceTempView(tempViewName)

    val appleTable: AppleTable = new AppleTable(SourceType.JDBC, jdbcDF, delimiter.getOrElse(null), tempViewName)
    return appleTable
  }

  /**
    * This method will create an AppleTable from a query that retrieves data from a database
    * accessed through JDBC connection.
    *
    * @param driverClassName JDBC driver name
    * @param jdbcUrl         JDBC URL
    * @param username        Username for database connection
    * @param password        Password for database connection
    * @param sqlQuery        Query to retrieve the desired data from database
    * @param tempViewName    temporary table name for source data
    * @param delimiter       source data separation character
    * @return custom table containing the data to be compared
    */
  def parallelizeJDBCSource(driverClassName: String, jdbcUrl: String, username: String, password: String, sqlQuery: String,
                            tempViewName: String, delimiter: Option[String], partitionColumn: String
                            , lowerBound: String, upperBound: String, numPartitions: String): AppleTable = {
    val jdbcDF: DataFrame = sparkSession.sqlContext.read
      .format("jdbc")
      .option("driver", driverClassName)
      .option("url", jdbcUrl)
      .option("dbtable", sqlQuery)
      .option("user", username)
      .option("password", password)
      .option("partitionColumn", partitionColumn)
      .option("lowerBound", lowerBound)
      .option("upperBound", upperBound)
      .option("numPartitions", numPartitions)
      .load()
    jdbcDF.createOrReplaceTempView(tempViewName)

    val appleTable: AppleTable = new AppleTable(SourceType.JDBC, jdbcDF, delimiter.getOrElse(null), tempViewName)
    return appleTable
  }

  /**
    * This method will create an AppleTable from a query that retrieves data from a database
    * accessed through JDBC connection; passes in "," as a default delimiter
    *
    * @param driverClassName JDBC driver name
    * @param jdbcUrl         JDBC URL
    * @param username        Username for database connection
    * @param password        Password for database connection
    * @param sqlQuery        Query to retrieve the desired data from database
    * @param tempViewName    temporary table name for source data
    * @return
    */
  def parallelizeJDBCSource(driverClassName: String, jdbcUrl: String, username: String, password: String, sqlQuery: String,
                            tempViewName: String): AppleTable = {
    parallelizeJDBCSource(driverClassName, jdbcUrl, username, password, sqlQuery, tempViewName, Option.apply(","))
  }

  /**
    * This method will create an AppleTable from a query that retrieves data from a hive
    * table.  It is assumed that hive connectivity is already enabled in the environment from
    * which this project is run.
    *
    * @param sqlText      a query to retrieve the data
    * @param tempViewName custom table name for source
    * @return custom table containing the data
    */
  def parallelizeHiveSource(sqlText: String, tempViewName: String): AppleTable = {
    val df: DataFrame = sparkSession.sql(sqlText)
    df.createOrReplaceTempView(tempViewName)
    val a: AppleTable = new AppleTable(SourceType.HIVE, df, ",", tempViewName)
    return a
  }

  /**
    * This method will create an AppleTable for data in DynamoDB table.
    *
    * @param tableName    name of DynamoDB table
    * @param tempViewName temporary table name for source data
    * @param jobConfMap   job configuration parameters for DynamoDB EMR connector
    * @param delimiter    source data separation character
    * @return custom table containing the data to be compared
    */
  def parallelizeDynamoDBSource(tableName: String, tempViewName: String, jobConfMap: mutable.HashMap[String, String],
                                delimiter: Option[String]): AppleTable = {
    val jobConf = new JobConf(sparkSession.sparkContext.hadoopConfiguration)
    jobConf.set("dynamodb.input.tableName", tableName)

    for (x <- jobConfMap) {
      jobConf.set(x._1, x._2)
    }

    if (jobConf.get("dynamodb.customAWSCredentialsProvider") == null) {
      jobConf.set("dynamodb.customAWSCredentialsProvider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    }

    val hadoopRDD: RDD[(Text, DynamoDBItemWritable)] =
      sparkSession.sparkContext.hadoopRDD(
        jobConf, classOf[DynamoDBInputFormat], classOf[Text], classOf[DynamoDBItemWritable])
    val values = hadoopRDD.values.map(x => x.getItem)
      .map(x => {
        new Gson().toJson(x)
          .replace("{\"nULLValue\":true}", "null")
      })

    val df = sparkSession.sqlContext.read.json(sparkSession.createDataset(values)(Encoders.STRING))

    df.createOrReplaceTempView(tempViewName)

    AppleTable(SourceType.DYNAMODB, df, delimiter.orNull, tempViewName)
  }

  /**
    * This method will create an AppleTable for data in DynamoDB table.
    *
    * @param tableName    name of DynamoDB table
    * @param tempViewName temporary table name for source data
    * @param jobConfMap   job configuration parameters for DynamoDB EMR connector
    * @return custom table containing the data to be compared
    */
  def parallelizeDynamoDBSource(tableName: String, tempViewName: String, jobConfMap: mutable.HashMap[String, String]): AppleTable = {
    parallelizeDynamoDBSource(tableName, tempViewName, jobConfMap, Option.apply(","))
  }

  /**
    * Flattens a DataFrame to only have a single column that contains the entire original row
    *
    * @param df        a dataframe which contains one or more columns
    * @param delimiter a character which separates the source data
    * @return flattened dataframe
    */
  def flattenDataFrame(df: DataFrame, delimiter: String): DataFrame = {
    //There is a bug open for this in the Spark Bug tracker
    val x = SparkFactory.sparkSession
    import x.implicits._

    val flatLeft = df.map(row => row.mkString(delimiter)).toDF("values")
    return flatLeft
  }

  /**
    * Removes null values stored in mixed data type nested structure dataframes that have been flattened
    *
    * @param df a dataframe flattened from mixed data type nested structure
    * @return flattened dataframe without mixed data type null values
    */
  def removeComplexNullFromFlatDataFrame(df: DataFrame): DataFrame = {
    val x = SparkFactory.sparkSession
    import x.implicits._

    df.map(row =>
      (for (x <- row.mkString.split("]"))
        yield x.replace("null,", "").replace(",null", "")
      ).mkString("]") + "]"
    ).toDF("values")
  }

  /**
    * Combine the schemas of two JSON format schemas and remove duplicate columns.
    * @param structTypeLeft   left table schema
    * @param structTypeRight  right table schema
    * @return combined schema without duplicate columns
    */
  def combineJSONFormatSchema(structTypeLeft: StructType, structTypeRight: StructType): StructType = {
    StructType(combineJSONFormatSchemaInner(StructType(structTypeLeft ++ structTypeRight)))
  }

  /**
    * Recursive helper function for combineJSONFormatSchema.
    * @param structType combined schema with duplicate columns
    * @return combined schema without duplicate columns
    */
  def combineJSONFormatSchemaInner(structType: StructType): StructType = {
    StructType(
      structType
        .groupBy(x => x.name)
        .flatMap(x => {
          val types = x._2.distinct
          if (types.length > 1) {
            StructType(
              Seq(StructField(x._1,
                if (x._2.head.dataType.typeName.equals("array") | x._2.last.dataType.typeName.equals("array"))
                  ArrayType(
                  combineJSONFormatSchemaInner(
                  StructType(
                    (for (x <- x._2)
                      yield
                        if (x.dataType.typeName.equals("array"))
                          for (field <- x.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
                            yield field
                        else if (x.dataType.typeName.equals("struct"))
                          for (field <- x.dataType.asInstanceOf[StructType])
                            yield field
                        else
                          null
                    ).filter(x => x != null).flatten
                  )
                ))
                else
                  combineJSONFormatSchemaInner(
                    StructType(
                      (for (x <- x._2)
                        yield
                          if (x.dataType.typeName.equals("struct"))
                            for (field <- x.dataType.asInstanceOf[StructType])
                              yield field
                          else
                            Seq(StructField(x.name, StringType, nullable = true))
                        ).flatten
                    )
                  )
                , nullable = true
              ))
            )
          }
          else types
        }).toSeq
    )
  }

  /**
    * Synchronize the schemas of two JSON format DataFrames.
    * @param leftDf   left JSON format DataFrame
    * @param rightDf  right JSON format DataFrame
    * @return left and right JSON format DataFrames with same schema
    */
  def syncJSONFormatSchemaDataFrame(leftDf: DataFrame, rightDf: DataFrame): (DataFrame, DataFrame) = {
    val schema = combineJSONFormatSchema(leftDf.schema, rightDf.schema)

    (
      sparkSession.read.schema(schema).json(leftDf.toJSON),
      sparkSession.read.schema(schema).json(rightDf.toJSON)
    )
  }

  /**
    * Combine all non-nested lower level values of a JSON format DataFrame into a common structure named "value".
    * @param df JSON format DataFrame
    * @return simplified JSON format DataFrame
    */
  def complexJSONFormatTableToSimpleJSONFormatTable(df: DataFrame): DataFrame = {
    var expandDf = df

    val schemaArray = df.schema.toArray

    for (colName <- df.columns
    ) {
      expandDf = expandDf.withColumn(colName,
        if (schemaArray(df.schema.fieldIndex(colName)).dataType.typeName.equals("struct"))
          complexJSONFormatTableToSimpleJSONFormatTableStruct(colName, functions.col(colName),
            schemaArray(df.schema.fieldIndex(colName)).dataType.asInstanceOf[StructType])
        else
          functions.col(colName)
      )
    }

    expandDf
  }

  /**
    * Helper function for complexJSONFormatTableToSimpleJSONFormatTable.
    * @param colName  name of column being processed
    * @param col      column being processed
    * @param schema   schema being processed
    * @return simplified column
    */
  def complexJSONFormatTableToSimpleJSONFormatTableStruct(colName: String, col: Column, schema: StructType): Column = {
    functions.struct(
      (
        (for (x <- schema.filter(x => x.dataType.typeName.startsWith("array")))
        yield
          col(x.name).alias(x.name)
        ) :+
        (if (schema.exists(x => !x.dataType.typeName.startsWith("array")))
          (if (col == null) functions.lit(null)
          else
            getCombinedValue(schema.filter(x => !x.dataType.typeName.startsWith("array") & !x.name.equals("nULLValue")), col)
          ).alias("value")
        else null
          )
        ).filter(x => x != null):_*
    )
  }

  /**
    * Helper function for complexJSONFormatTableToSimpleJSONFormatTableStruct.
    * @param schema schema being processed
    * @param col    column being processed
    * @return simplified column
    */
  def getCombinedValue(schema: Seq[StructField], col: Column): Column = {
    if (schema.nonEmpty)
      functions.when(col(schema.head.name).isNotNull, col(schema.head.name).cast("string"))
        .otherwise(
          if (schema.length > 1) getCombinedValue(schema.toArray.slice(1, schema.length), col)
          else functions.lit(null)
        )
    else
      functions.lit(null)
  }

  /**
    * Transform single level DataFrame to a JSON format DataFrame
    * @param df single level DataFrame
    * @return simplified JSON format DataFrame
    */
  def simpleTableToSimpleJSONFormatTable(df: DataFrame): DataFrame = {
    var expandDf = df

    for (colName <- df.columns) {
      expandDf = expandDf.withColumn(colName, functions.struct(functions.col(colName).cast("string").alias("value")))
    }

    expandDf
  }

  object sparkImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = SparkFactory.sparkSession.sqlContext
  }
}

