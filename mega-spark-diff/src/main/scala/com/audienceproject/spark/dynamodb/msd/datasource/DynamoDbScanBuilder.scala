package com.audienceproject.spark.dynamodb.msd.datasource

import com.audienceproject.spark.dynamodb.connector.{DynamoConnector, FilterPushdown}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class DynamoDbScanBuilder(connector: DynamoConnector, schema: StructType)
  extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters {

  private var pushedFilter = Array.empty[Filter]
  private var finalSchema = schema

  /**
   * code from <a href="https://github.com/audienceproject/spark-dynamodb">com.audienceproject:spark.dynamodb</a>
   * <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/src/main/scala/com/audienceproject/spark/dynamodb/datasource/DynamoScanBuilder.scala">DynamoScanBuilder</a>
   *
   * @return DynamoDbScan instance
   */
  override def build(): Scan = new DynamoDbScan(connector, pushedFilters(), finalSchema)

  /**
   * code from <a href="https://github.com/audienceproject/spark-dynamodb">com.audienceproject:spark.dynamodb</a>
   * <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/src/main/scala/com/audienceproject/spark/dynamodb/datasource/DynamoScanBuilder.scala">DynamoScanBuilder</a>
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    val keyColumns = Seq(Some(connector.keySchema.hashKeyName), connector.keySchema.rangeKeyName).flatten
      .flatMap(keyName => finalSchema.fields.find(_.name == keyName))
    val requiredColumns = keyColumns ++ requiredSchema.fields
    val newFields = finalSchema.fields.filter(requiredColumns.contains)
    finalSchema = StructType(newFields)
  }

  /**
   * code from <a href="https://github.com/audienceproject/spark-dynamodb">com.audienceproject:spark.dynamodb</a>
   * <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/src/main/scala/com/audienceproject/spark/dynamodb/datasource/DynamoScanBuilder.scala">DynamoScanBuilder</a>
   *
   * @return array of filters
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (connector.filterPushdownEnabled) {
      val (acceptedFilters, postScanFilters) = FilterPushdown.acceptFilters(filters)
      this.pushedFilter = acceptedFilters
      postScanFilters
    } else {
      filters
    }
  }

  /**
   * code from <a href="https://github.com/audienceproject/spark-dynamodb">com.audienceproject:spark.dynamodb</a>
   * <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/src/main/scala/com/audienceproject/spark/dynamodb/datasource/DynamoScanBuilder.scala">DynamoScanBuilder</a>
   *
   * @return array of filters
   */
  override def pushedFilters(): Array[Filter] = pushedFilter
}
