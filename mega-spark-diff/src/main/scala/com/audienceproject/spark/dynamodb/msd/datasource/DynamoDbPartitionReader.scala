package com.audienceproject.spark.dynamodb.msd.datasource

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.util.json.Jackson
import com.audienceproject.spark.dynamodb.connector.DynamoConnector
import com.audienceproject.spark.dynamodb.datasource.ScanPartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters.asScalaIteratorConverter

class DynamoDbPartitionReader(connector: DynamoConnector, schema: StructType, partition: ScanPartition) extends PartitionReader[InternalRow] {

  // code based on com.audienceproject:spark.dynamodb
  private val pageIterator =
    connector.scan(partition.partitionIndex, partition.requiredColumns, partition.filters).pages().iterator().asScala

  private var rowIterator = Iterator[Item]()
  private var result = new Item

  override def next(): Boolean = {
    if (rowIterator.hasNext) {
      result = rowIterator.next()
      return true
    }
    else if (pageIterator.hasNext) {
      rowIterator = pageIterator.next().getLowLevelResult.getItems.iterator().asScala
      return next()
    }
    else return false
    true
  }

  override def get(): InternalRow = {
    val resultRow = for (x <- schema) yield
      if (result.isNull(x.name) || result.get(x.name) == null) null
      else UTF8String.fromString(
        result.get(x.name) match {
          case value: MapAny => toJSON(value)
          case value: ListAny => toJSON(value)
          case value: SetAny => toJSON(value)
          case _ => toJSON(result.get(x.name))
        })
    val row = InternalRow(resultRow:_*)
    row
  }

  private def toJSON(value: Any): String = {
    Jackson.toJsonString(value)
  }

  override def close(): Unit = Unit
}
