package org.apache.spark.sql.execution.datasources.msd

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, JsonNodeType, ObjectNode}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriterFactory, PartitionedFile, TextBasedFileFormat}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import java.util
import java.util.Collections

  class DefaultSource extends TextBasedFileFormat with DataSourceRegister {
    override val shortName: String = "jsonmsd"

    override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = null

    override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = null

    /**
     * code based on <a href="https://github.com/apache/spark">org.apache.spark:spark-core</a>
     * <a href="https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonFileFormat.scala">JsonFileFormat</a>
     *
     * @param sparkSession sparkSession
     * @param dataSchema dataSchema
     * @param partitionSchema partitionSchema
     * @param requiredSchema requiredSchema
     * @param filters filters
     * @param options options
     * @param hadoopConf hadoopConf
     * @return PartitionedFile => Iterator[InternalRow]
     */
    override protected def buildReader(sparkSession: SparkSession,
                                       dataSchema: StructType,
                                       partitionSchema: StructType,
                                       requiredSchema: StructType,
                                       filters: Seq[Filter],
                                       options: Map[String, String],
                                       hadoopConf: Configuration
                                      ): PartitionedFile => Iterator[InternalRow] = {
      val broadcastedHadoopConf =
        sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
      (file: PartitionedFile) => {
        JsonFileFormatMsd.readFile(
          broadcastedHadoopConf.value.value,
          file,
          requiredSchema
        )
      }
    }
  }

  class JsonNodeFactorySortedKeys extends JsonNodeFactory {
    override def objectNode(): ObjectNode =
      new ObjectNode(this, new util.TreeMap[String, JsonNode]())
  }

  object JsonFileFormatMsd extends Serializable {
    def readFile(
                  conf: Configuration,
                  file: PartitionedFile,
                  schema: StructType): Iterator[InternalRow] = {
      val inputStream = CodecStreams.createInputStreamWithCloseResource(conf, file.toPath)

      val objectMapper = JsonMapper.builder().nodeFactory(new JsonNodeFactorySortedKeys()).build()

      val json = objectMapper.readTree(inputStream)

      val jsonIterator = json.getNodeType match {
        case JsonNodeType.ARRAY => json.iterator()
        case _ => Collections.singletonList(json).iterator()
      }

      new Iterator[InternalRow] {
        override def hasNext: Boolean = jsonIterator.hasNext

        override def next(): InternalRow = {
          val row = jsonIterator.next().fields()
          val internalRow = new GenericInternalRow(schema.length)

          while (row.hasNext) {
            val item = row.next()

            val fieldIndex = schema.getFieldIndex(item.getKey)
            if (fieldIndex.getOrElse(-1) != -1) {
              if (item.getValue.isNull)
                internalRow.update(fieldIndex.get, null)
              else
                internalRow.update(fieldIndex.get, UTF8String.fromString(item.getValue.toString))
            }
          }
          internalRow
        }
      }
    }
  }
