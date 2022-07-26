package org.finra.msd.implicits

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object DataFrameImplicits {

  implicit class DataFrameImprovements(df: DataFrame) {
    def getColumnsSeq(): Seq[Column] = {
      val s: Seq[Column] = df.columns.map(c => df(c)).toSeq
      return s
    }
  }



}
