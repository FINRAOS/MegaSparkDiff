package org.finra.msd.basetestclasses

import org.apache.spark.sql.SparkSession
import org.finra.msd.sparkfactory.SparkFactory
import org.scalatest.{BeforeAndAfterAll, FeatureSpec}

trait SparkSessionTrait extends FeatureSpec with BeforeAndAfterAll {

  SparkFactory.initializeSparkLocalMode("local[*]" , "WARN","1")
  val sparkSession :SparkSession = SparkFactory.sparkSession

  override def afterAll(): Unit = {
    sparkSession.stop()
    super.afterAll()
  }

}
