/*
 * Copyright 2017 MegaSparkDiff Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finra.msd.outputwriters

import org.apache.spark.sql.DataFrame

object OutputWriter {

  /**
   * Stores comparison results locally
   *
   * @param leftResult      a dataframe which contains the values in RDD1 and not in RDD2
   * @param rightResult     a dataframe which contains the values in RDD2 and not in RDD1
   * @param outputDirectory location where the comparison results are to be stored
   * @param singleFile      a boolean variable to denote the number of output files to be one or more than one
   */
  def saveResultsToDisk(leftResult: DataFrame, rightResult: DataFrame,
                        outputDirectory: String, singleFile: Boolean, delimiter: String): Unit = {
    var left: DataFrame = leftResult
    var right: DataFrame = rightResult


    if (singleFile) {
      left = leftResult.coalesce(1)
      right = rightResult.coalesce(1)
    }

    // Write the symmetric difference to their own output directories
    val header: Boolean = true
    left.write.format("com.databricks.spark.csv").option("header", header + "").option("delimiter", delimiter).mode("overwrite").save(outputDirectory + "/inLeftNotInRight")
    right.write.format("com.databricks.spark.csv").option("header", header + "").option("delimiter", delimiter).mode("overwrite").save(outputDirectory + "/inRightNotInLeft")

  }


}
