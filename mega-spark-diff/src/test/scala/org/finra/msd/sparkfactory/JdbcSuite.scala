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

package org.finra.msd.sparkfactory

import org.finra.msd.basetestclasses.SparkFunSuite
import org.finra.msd.enums.SourceType
import org.finra.msd.helpers.FileHelper
import org.finra.msd.memorydb.MemoryDbHsql

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class JdbcSuite extends SparkFunSuite {

  val directory = "jdbc"
  val expectedSourceType = SourceType.JDBC

  FileHelper.getFilenames(directory, "JdbcTest", ".sql")
    .foreach(filename => {
    test("SparkFactory Jdbc test - " + filename) {
      val baseFile = filename.substring(0, filename.length - 4)

      val tableName = baseFile.replace(directory + "/", "").toUpperCase()

      val appleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
        "(select * from " + tableName + ")", tableName + "_jdbc_test")

      val jsonFormatDf = SparkFactory.simpleTableToSimpleJSONFormatTable(appleTable.dataFrame)

      assert(appleTable.getSourceType.equals(expectedSourceType))

      helpers.compareJdbcDataFrameSimpleToJsonFormat(appleTable, jsonFormatDf, Seq("Fruit"))
    }
  })

  test("parallelizeSqlQueryTest") {
    val appleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from Persons1)", "table1")
    if (appleTable.getDataFrame.count == 0) fail("dataset was empty")
  }

  test("parrallelizeSqlQueryWithPartitioning") {
    val rightAppleTable = SparkFactory.parallelizeJDBCSource(MemoryDbHsql.hsqlDriverName, MemoryDbHsql.hsqlUrl, "SA", "",
      "(select * from Fruit1 )", "my_partition_test", Option.empty, "Price", "0", "200000", "2")
    if (rightAppleTable.getDataFrame.rdd.getNumPartitions != 2) fail("expected 2 partitions but received " + rightAppleTable.getDataFrame.rdd.getNumPartitions)
  }
}
