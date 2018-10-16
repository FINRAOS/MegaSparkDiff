package org.finra.msd.outputwriters

import org.apache.spark.sql.DataFrame

object OutputWriter {

  /**
    * Stores comparison results locally
    *
    * @param leftResult a dataframe which contains the values in RDD1 and not in RDD2
    * @param rightResult a dataframe which contains the values in RDD2 and not in RDD1
    * @param outputDirectory location where the comparison results are to be stored
    * @param singleFile a boolean variable to denote the number of output files to be one or more than one
    */
  def saveResultsToDisk(leftResult: DataFrame , rightResult: DataFrame,
                                outputDirectory: String , singleFile: Boolean , delimiter :String) :Unit =
  {
    var left: DataFrame = leftResult
    var right: DataFrame = rightResult


    if (singleFile)
    {
      left = leftResult.coalesce(1)
      right = rightResult.coalesce(1)
    }

    // Write the symmetric difference to their own output directories
    val header : Boolean = true
    left.write.format("com.databricks.spark.csv").option("header", header + "").option("delimiter",delimiter).mode("overwrite").save(outputDirectory + "/inLeftNotInRight")
    right.write.format("com.databricks.spark.csv").option("header", header + "").option("delimiter",delimiter).mode("overwrite").save(outputDirectory + "/inRightNotInLeft")

  }


}
