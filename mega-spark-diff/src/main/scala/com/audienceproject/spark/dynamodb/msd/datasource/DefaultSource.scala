package com.audienceproject.spark.dynamodb.msd.datasource

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.finra.msd.sparkfactory.SparkFactory.sparkSession

import java.util

class DefaultSource extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    throw new IllegalArgumentException(
      s"dynamodb does not support inferred schema. Please specify the schema.")

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new DynamoDbTable(options = new CaseInsensitiveStringMap(properties), schema)

  override def supportsExternalMetadata(): Boolean = true
}
