package com.audienceproject.spark.dynamodb.msd.datasource

import com.audienceproject.spark.dynamodb.connector.DynamoConnector
import com.audienceproject.spark.dynamodb.datasource.ScanPartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class DynamoDbPartitionReaderFactory(connector: DynamoConnector, schema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new DynamoDbPartitionReader(connector, schema, partition.asInstanceOf[ScanPartition])
}
