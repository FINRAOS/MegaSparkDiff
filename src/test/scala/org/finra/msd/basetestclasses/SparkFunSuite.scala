/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finra.msd.basetestclasses

// scalastyle:off
import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SQLImplicits}
import org.finra.msd.memorydb.MemoryDbHsql
import org.finra.msd.sparkfactory.SparkFactory
import org.scalatest._

import scala.reflect.io.{File, Path}
import scala.util.Try

class SparkFunSuite
  extends FunSuite
    with BeforeAndAfterAll
    with Logging
    with Matchers
    with SharedSqlContext
    with ParallelTestExecution {


  protected val outputDirectory: String = System.getProperty("user.dir") + "/sparkOutputDirectory"
  protected final val hsqlDriverName = "org.hsqldb.jdbc.JDBCDriver"
  protected final val hsqlUrl = "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb"

  private lazy val sparkSession = SparkFactory.sparkSession

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = SparkFactory.sparkSession.sqlContext
  }

  implicit class SequenceImprovements(seq: Seq[Row]) {
    def toDf(schema: StructType): DataFrame = {
      val rowRdd = sparkSession.sparkContext.parallelize(seq)
      val df = sparkSession.createDataFrame(rowRdd, schema)
      return df
    }
  }

  override def beforeAll(): Unit = synchronized {

    SparkFactory.initializeSparkLocalMode("local[*]", "WARN", "1")

    if (MemoryDbHsql.getInstance.getState != 1) {
      MemoryDbHsql.getInstance.initializeMemoryDB()
    }
    super.beforeAll()
  }


  override def afterAll(): Unit = {
    super.afterAll()
  }

  // helper function
  protected final def getTestResourceFile(file: String): java.io.File = {
    new java.io.File(getClass.getClassLoader.getResource(file).getFile)
  }

  protected final def getTestResourcePath(file: String): String = {
    getTestResourceFile(file).getCanonicalPath
  }

  /**
    * Log the suite name and the test name before and after each test.
    *
    * Subclasses should never override this method. If they wish to run
    * custom code before and after each test, they should mix in the
    * {{org.scalatest.BeforeAndAfter}} trait instead.
    */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

  def deleteSavedFiles(folder : String): Unit = {
    val path: Path = Path(outputDirectory + "/" + folder + "/")
    Try(path.deleteRecursively())
  }

def readSavedFile(folder : String): String = {
    val file = new java.io.File(outputDirectory + "/" + folder + "/")
      .listFiles
      .filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))(0)

    scala.reflect.io.File(file).slurp()
  }
}
