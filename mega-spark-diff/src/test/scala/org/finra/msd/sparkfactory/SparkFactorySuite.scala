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
 *//*
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


class SparkFactorySuite extends SparkFunSuite {

  test("parallelizeSqlQueryTest")
  {
    val appleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb", "SA", "", "(select * from Persons1)", "table1")
    if (appleTable.getDataFrame.count == 0) fail("dataset was empty")
  }

  test("parrallelizeSqlQueryWithPartitioning") {
    val rightAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb", "SA", "", "(select * from Test1 )", "my_partition_test", scala.Option.empty, "Price", "0", "200000", "2")
    if (rightAppleTable.getDataFrame.rdd.getNumPartitions != 2) fail("expected 2 partitions but received " + rightAppleTable.getDataFrame.rdd.getNumPartitions)
  }
}