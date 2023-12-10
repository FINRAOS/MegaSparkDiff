package com.audienceproject.spark.dynamodb.msd.datasource

import com.audienceproject.spark.dynamodb.connector.{TableConnector, TableIndexConnector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class DynamoDbTable(options: CaseInsensitiveStringMap, schema: StructType) extends Table with SupportsRead {
  // code from com.audienceproject:spark.dynamodb
  private val dynamoConnector = {
    val indexName = Option(options.get("indexname"))
    val defaultParallelism = Option(options.get("defaultparallelism")).map(_.toInt).getOrElse(getDefaultParallelism)
    val optionsMap = Map(options.asScala.toSeq: _*)

    if (indexName.isDefined) new TableIndexConnector(name(), indexName.get, defaultParallelism, optionsMap)
    else new TableConnector(name(), defaultParallelism, optionsMap)
  }

  // code from com.audienceproject:spark.dynamodb
  private def getDefaultParallelism: Int =
    SparkSession.getActiveSession match {
      case Some(spark) => spark.sparkContext.defaultParallelism
    }

  override def name(): String = options.get("table")

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new DynamoDbScanBuilder(dynamoConnector, schema())
}
