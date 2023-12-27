package com.audienceproject.spark.dynamodb.msd.datasource

import com.audienceproject.spark.dynamodb.connector.DynamoConnector
import com.audienceproject.spark.dynamodb.datasource.ScanPartition
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

class DynamoDbScan(connector: DynamoConnector, filters: Array[Filter], schema: StructType)
  extends Scan with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  /**
   * code based on <a href="https://github.com/audienceproject/spark-dynamodb">com.audienceproject:spark.dynamodb</a>
   * <a href="https://github.com/audienceproject/spark-dynamodb/blob/master/src/main/scala/com/audienceproject/spark/dynamodb/datasource/DynamoBatchReader.scala">DynamoBatchReader</a>
   *
   * @return array of input partitions
   */
  override def planInputPartitions(): Array[InputPartition] = {
    Array.tabulate(connector.totalSegments)(new ScanPartition(_, schema.fieldNames, filters))
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new DynamoDbPartitionReaderFactory(connector, schema)
}
