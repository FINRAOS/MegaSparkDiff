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
import org.apache.spark.sql.functions.{concat, _}
import org.apache.spark.sql.types._
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
  def initializeSparkContext(): Unit = {
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
        sparkSession = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
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
   * @return                 a dataframe converted from a flat file
   */
  def parallelizeTextFile(textFileLocation: String): DataFrame = {
    //parallelize a text file and map each line to an untyped Row object hence getting an RDD of type Row
    val rowRDD: RDD[Row] = sparkSession.sparkContext.textFile(textFileLocation).map((row: String) => RowFactory.create(row))
    // Creates a schema of a single column called Values of type String
    val schema: StructType = DataTypes.createStructType(Array[StructField](DataTypes.createStructField("values", DataTypes.StringType, true)))
    //Ask spark SQL to create a dataFrame based on the RDD[Row]
    sparkSession.sqlContext.createDataFrame(rowRDD, schema).toDF
  }

  /**
   * This method will create DataFrame with a single column called "field1" from a text file, the file can be on local
   * machine or HDFS for HDFS url like so
   * "hdfs://nn1home:8020/input/war-and-peace.txt" for S3 the url like so
   * "s3n://myBucket/myFile1.log"
   *
   * @param textFileLocation path of a flat file containing the data to be compared
   * @param tempViewName     temporary table name for source data
   * @return                 custom table containing the data to be compared
   */
  def parallelizeTextSource(textFileLocation: String, tempViewName: String): AppleTable = {
    val df = parallelizeTextFile(textFileLocation)
    AppleTable(SourceType.FILE, df, null, tempViewName)
  }

  /** This method will create an AppleTable for data in a JSON file.
   *
   * @param jsonFileLocation       path of a json file containing the data to be compared
   * @param tempViewName           temporary table name for source data
   * @param firstLevelElementNames Names of the first level elements in the file
   * @param delimiter              source data separation character
   * @return                       custom table containing the data to be compared
   */
  def parallelizeJSONSource(jsonFileLocation: String, tempViewName: String,
                            firstLevelElementNames: Array[String], delimiter: Option[String] = Option.apply(",")): AppleTable = {
    val expectedSchema = new StructType(firstLevelElementNames.map(x => StructField(x, StringType, nullable = true)))

    val df = sparkSession.sqlContext.read
      .schema(expectedSchema)
      .option("multiLine", "true")
      .format("org.apache.spark.sql.execution.datasources.msd")
      .load(jsonFileLocation)
    df.createOrReplaceTempView(tempViewName)

    AppleTable(SourceType.JSON, df, delimiter.orNull, tempViewName)
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
   * @param iamAuth         flag indicating whether to use IAM authentication
   * @param region          AWS region for IAM authentication
   * @return                custom table containing the data to be compared
   */
  def parallelizeJDBCSource(driverClassName: String, jdbcUrl: String, username: String, password: String,
                            sqlQuery: String, tempViewName: String, delimiter: Option[String],
                            iamAuth: Option[String], region: Option[String]): AppleTable = {
    val reader = sparkSession.sqlContext.read
      .format("jdbc")
      .option("driver", driverClassName)
      .option("url", jdbcUrl)
      .option("dbtable", sqlQuery)
      .option("user", username)
      .option("password", password)
      .option("iamAuth", iamAuth.getOrElse("false"))
      .option("region", region.getOrElse(""))

    // Only add connectionProvider for PostgreSQL driver
    val readerWithProvider = if (driverClassName == "org.postgresql.Driver") {
      reader.option("connectionProvider", "PostgresIamAuthConnectionProvider")
    } else {
      reader
    }

    val jdbcDF: DataFrame = readerWithProvider.load()
    jdbcDF.createOrReplaceTempView(tempViewName)

    AppleTable(SourceType.JDBC, jdbcDF, delimiter.orNull, tempViewName)
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
   * @param iamAuth         flag indicating whether to use IAM authentication
   * @param region          AWS region for IAM authentication
   * @param partitionColumn column to partition queries on
   * @param lowerBound      lower bound of partition column values
   * @param upperBound      upper bound of partition column values
   * @param numPartitions   number of total queries to execute
   * @return                custom table containing the data to be compared
   */
  def parallelizeJDBCSource(driverClassName: String, jdbcUrl: String, username: String, password: String, sqlQuery: String,
                            tempViewName: String, delimiter: Option[String], iamAuth: Option[String], region: Option[String],
                            partitionColumn: String, lowerBound: String, upperBound: String, numPartitions: String): AppleTable = {
    val reader = sparkSession.sqlContext.read
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
      .option("iamAuth", iamAuth.getOrElse("false"))
      .option("region", region.getOrElse(""))

    // Only add connectionProvider for PostgreSQL driver
    val readerWithProvider = if (driverClassName == "org.postgresql.Driver") {
      reader.option("connectionProvider", "PostgresIamAuthConnectionProvider")
    } else {
      reader
    }

    val jdbcDF: DataFrame = readerWithProvider.load()
    jdbcDF.createOrReplaceTempView(tempViewName)

    AppleTable(SourceType.JDBC, jdbcDF, delimiter.orNull, tempViewName)
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
   * @return                custom table containing the data to be compared
   */
  def parallelizeJDBCSource(driverClassName: String, jdbcUrl: String, username: String, password: String, sqlQuery: String,
                            tempViewName: String): AppleTable = {
    parallelizeJDBCSource(driverClassName, jdbcUrl, username, password, sqlQuery, tempViewName, Option.apply(","), Option.empty, Option.empty)
  }

  /**
   * This method will create an AppleTable from a query that retrieves data from a hive
   * table.  It is assumed that hive connectivity is already enabled in the environment from
   * which this project is run.
   *
   * @param sqlText      a query to retrieve the data
   * @param tempViewName custom table name for source
   * @return             custom table containing the data
   */
  def parallelizeHiveSource(sqlText: String, tempViewName: String): AppleTable = {
    val df: DataFrame = sparkSession.sql(sqlText)
    df.createOrReplaceTempView(tempViewName)
    AppleTable(SourceType.HIVE, df, ",", tempViewName)
  }

  /**
   * This method will create an AppleTable for data in DynamoDB table.
   *
   * The method uses the <a href="https://github.com/audienceproject/spark-dynamodb">spark-dynamodb</a> library, of which
   * many of the parameters below are passed to, hence the inclusion of their descriptions from the
   * <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">spark-dynamodb README file</a>
   *
   * @param tableName               name of DynamoDB table
   * @param tempViewName            temporary table name for source data
   * @param firstLevelElementNames  names of the first level elements in the table
   * @param delimiter               source data separation character
   * @param selectColumns           list of columns to select from table
   * @param filter                  sql where condition
   * @param region                  region Spark parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "sets the region where the dynamodb table. Default is environment specific."
   * @param roleArn                 roleArn Spark parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "sets an IAM role to assume. This allows for access to a DynamoDB in a different account than the Spark cluster. Defaults to the standard role configuration."
   * @param readPartitions          readPartitions Spark reader parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "number of partitions to split the initial RDD when loading the data into Spark. Defaults to the size of the DynamoDB table divided into chunks of maxPartitionBytes"
   * @param maxPartitionBytes       maxPartitionBytes Spark reader parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "the maximum size of a single input partition. Default 128 MB"
   * @param defaultParallelism      defaultParallelism Spark reader parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "the number of input partitions that can be read from DynamoDB simultaneously. Defaults to sparkContext.defaultParallelism"
   * @param targetCapacity          targetCapacity Spark reader parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "fraction of provisioned read capacity on the table (or index) to consume for reading. Default 1 (i.e. 100% capacity)."
   * @param stronglyConsistentReads stronglyConsistentReads Spark reader parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "whether or not to use strongly consistent reads. Default false."
   * @param bytesPerRCU             bytesPerRCU Spark reader parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "number of bytes that can be read per second with a single Read Capacity Unit. Default 4000 (4 KB). This value is multiplied by two when stronglyConsistentReads=false"
   * @param filterPushdown          filterPushdown Spark reader parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "whether or not to use filter pushdown to DynamoDB on scan requests. Default true."
   * @param throughput              throughput Spark reader  parameter with the following description from the
   *                                <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/README.md">source README</a>:
   *                                "the desired read throughput to use. It overwrites any calculation used by the package. It is intended to be used with tables that are on-demand. Defaults to 100 for on-demand."
   *
   * @return custom table containing the data to be compared
   */
  def parallelizeDynamoDBSource(tableName: String, tempViewName: String,
                                firstLevelElementNames: Array[String], delimiter: Option[String] = Option.apply(","),
                                selectColumns: Option[Array[String]] = Option.empty, filter: Option[String] = Option.empty,
                                region: Option[String] = Option.empty, roleArn: Option[String] = Option.empty,
                                readPartitions: Option[String] = Option.empty, maxPartitionBytes: Option[String] = Option.empty,
                                defaultParallelism: Option[String] = Option.empty, targetCapacity: Option[String] = Option.empty,
                                stronglyConsistentReads: Option[String] = Option.empty, bytesPerRCU: Option[String] = Option.empty,
                                filterPushdown: Option[String] = Option.empty, throughput: Option[String] = Option.empty): AppleTable = {
    val expectedSchema = new StructType(firstLevelElementNames.map(x => StructField(x, StringType, nullable = true)))

    val options = (
      Map("region" -> region) ++
      Map("roleArn" -> roleArn) ++
      Map("readPartitions" -> readPartitions) ++
      Map("maxPartitionBytes" -> maxPartitionBytes) ++
      Map("defaultParallelism" -> defaultParallelism) ++
      Map("targetCapacity" -> targetCapacity) ++
      Map("stronglyConsistentReads" -> stronglyConsistentReads) ++
      Map("bytesPerRCU" -> bytesPerRCU) ++
      Map("filterPushdown" -> filterPushdown) ++
      Map("throughput" -> throughput)
      ).filter(x => x._2.isDefined)
      .map(x => x._1 -> x._2.get)

    var df = sparkSession.read.schema(expectedSchema)
      .format("com.audienceproject.spark.dynamodb.msd.datasource")
      .option("table", tableName)
      .options(options)
      .load
      .select(selectColumns.getOrElse(firstLevelElementNames).map(x => new Column(x)): _*)

    if (filter.isDefined)
      df = df.filter(filter.get)

    df.createOrReplaceTempView(tempViewName)

    AppleTable(SourceType.DYNAMODB, df, delimiter.orNull, tempViewName)
  }

  /** This method creates an AppleTable for data in a CSV file
   *
   * @param filePath     the relative path to the CSV file
   * @param tempViewName the name of the temporary view which gets created for source data
   * @param schemaDef    optional schema definition for the data. If none is provided, the schema will be inferred by spark
   * @param delimiter    optional delimiter character for the data. If none is provided, comma (",") will be used as default
   * @return             custom table containing the data to be compared
   */
  def parallelizeCSVSource(filePath: String, tempViewName: String, schemaDef: Option[StructType] = Option.empty,
                           delimiter: Option[String] = Option.apply(",")): AppleTable ={

    var df: DataFrame = null

    //Command to read the data from CSV into DataFrame
    if(schemaDef.isDefined) {
      df = sparkSession.sqlContext.read
        .option("multiLine", "true")
        .option("delimiter", delimiter.get)
        .schema(schemaDef.get)
        .format("csv")
        .load(filePath)
    }
    else {
      //if caller does not pass in a schema, use the inferSchema option
      df = sparkSession.sqlContext.read
        .option("multiLine", "true")
        .option("delimiter", delimiter.get)
        .option("inferSchema", "true")
        .format("csv")
        .load(filePath)
    }

    df.createOrReplaceTempView(tempViewName)

    AppleTable(SourceType.CSV, df, delimiter.get, tempViewName)
  }

  /**
   * Flattens a DataFrame to only have a single column that contains the entire original row
   *
   * @param df        a dataframe which contains one or more columns
   * @param delimiter a character which separates the source data
   * @return          flattened dataframe
   */
  def flattenDataFrame(df: DataFrame, delimiter: String): DataFrame = {
    //There is a bug open for this in the Spark Bug tracker
    val x = SparkFactory.sparkSession
    import x.implicits._

    df.map(row => row.mkString(delimiter)).toDF("values")
  }

  /**
   * Transform single level DataFrame to a JSON format DataFrame
   * @param df single level DataFrame
   * @return   simplified JSON format DataFrame
   */
  def simpleTableToSimpleJSONFormatTable(df: DataFrame): DataFrame = {
    var expandDf = df

    for (field <- df.schema.fields) {
      if (field.dataType == StringType)
        expandDf = expandDf.withColumn(field.name,
          when(col(field.name).startsWith(lit("[")).and(col(field.name).endsWith(lit("]"))),
            col(field.name)
          ).otherwise(
            when(col(field.name).startsWith(lit("{")).and(col(field.name).endsWith(lit("}"))),
              col(field.name)
            ).otherwise(
              concat(lit("\""), col(field.name), lit("\""))
            )
          )
        )
    }

    expandDf
  }

  object sparkImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = SparkFactory.sparkSession.sqlContext
  }
}

