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

import org.finra.msd.basetestclasses.SparkFunSuiteDynamoDb
import org.finra.msd.enums.SourceType
import org.finra.msd.helpers.FileHelper
import org.finra.msd.sparkfactory.SparkFactory.sparkSession

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class DynamoDbSuite extends SparkFunSuiteDynamoDb {

  val directoryJson = "json"
  val directoryDynamoDb = "dynamodb"
  val expectedSourceType = SourceType.DYNAMODB

  (FileHelper.getFilenames(directoryJson, "JsonTest", ".json")
    ++ FileHelper.getFilenames(directoryDynamoDb, "DynamoDbTest", ".json"))
    .foreach(filename => {
    test("SparkFactory DynamoDb test - " + filename) {
      val baseFile = filename.substring(0, filename.length - 5)
      val tableName = baseFile.replaceAll("/", "_")
      val jsonFile = filename
      val txtFile = baseFile + ".txt"
      val jsonPath = this.getClass.getClassLoader.getResource(jsonFile).getPath
      val txtPath = this.getClass.getClassLoader.getResource(txtFile).getPath

      val schema = sparkSession.sqlContext.read
        .option("multiLine", "true")
        .option("primitivesAsString", "true")
        .json(jsonPath).schema

      val appleTable = SparkFactory.parallelizeDynamoDBSource(tableName, "dynamodb_test", schema.fieldNames)
      assert(appleTable.getSourceType.equals(expectedSourceType))

      helpers.compareJsonDataFrameActualToExpected(appleTable,
        if (baseFile.endsWith("SetDiffElementOrder"))
          jsonPath.replace("SetDiffElementOrder.json", "Set.json")
        else jsonPath,
        txtPath, Seq("key1", "key2"), ";")
    }
  }
  )

  test("Dynamodb column filter") {
    val jsonPath = this.getClass.getClassLoader.getResource("json/JsonTestSimple.json").getPath

    val schema = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(jsonPath).schema

    val appleTable = SparkFactory.parallelizeDynamoDBSource("json_JsonTestSimple", "dynamodb_test", schema.fieldNames,
      Option.apply(","), Option.apply(Array("key1", "attribute1")), Option.empty)
    assert(appleTable.getSourceType.equals(expectedSourceType))

    val dataResult = appleTable.getDataFrame.orderBy("key1").collect()
    assert(dataResult.length == 3)

    val schemaResult = appleTable.getDataFrame.schema
    assert(schemaResult.fields.length == 2)

    assert(dataResult.take(3)(0)(schemaResult.fieldIndex("key1")).equals("\"TEST1\""))
    assert(dataResult.take(3)(0)(schemaResult.fieldIndex("attribute1")).equals("\"test number 1\""))
    assert(dataResult.take(3)(1)(schemaResult.fieldIndex("key1")).equals("\"TEST2\""))
    assert(dataResult.take(3)(1)(schemaResult.fieldIndex("attribute1")).equals("\"test number 2\""))
    assert(dataResult.take(3)(2)(schemaResult.fieldIndex("key1")).equals("\"TEST3\""))
    assert(dataResult.take(3)(2)(schemaResult.fieldIndex("attribute1")).equals("\"test number 3\""))
  }

  test("Dynamodb row filter") {
    val jsonPath = this.getClass.getClassLoader.getResource("json/JsonTestSimple.json").getPath

    val schema = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(jsonPath).schema

    val appleTable = SparkFactory.parallelizeDynamoDBSource("json_JsonTestSimple", "dynamodb_test", schema.fieldNames,
      Option.apply(","), Option.empty, Option.apply("key1 = 'TEST3'"))
    assert(appleTable.getSourceType.equals(expectedSourceType))

    val dataResult = appleTable.getDataFrame.collect()
    assert(dataResult.length == 1)

    val schemaResult = appleTable.getDataFrame.schema
    assert(schemaResult.fields.length == 5)

    assert(dataResult.take(1)(0)(schemaResult.fieldIndex("key1")).equals("\"TEST3\""))
    assert(dataResult.take(1)(0)(schemaResult.fieldIndex("key2")).equals("3"))
    assert(dataResult.take(1)(0)(schemaResult.fieldIndex("attribute1")).equals("\"test number 3\""))
    assert(dataResult.take(1)(0)(schemaResult.fieldIndex("attribute2")).equals("\"true\""))
    assert(dataResult.take(1)(0)(schemaResult.fieldIndex("attribute3")).equals("\"3\""))
  }

  test("Dynamodb column and row filter") {
    val jsonPath = this.getClass.getClassLoader.getResource("json/JsonTestSimple.json").getPath

    val schema = sparkSession.sqlContext.read
      .option("multiLine", "true")
      .option("primitivesAsString", "true")
      .json(jsonPath).schema

    val appleTable = SparkFactory.parallelizeDynamoDBSource("json_JsonTestSimple", "dynamodb_test", schema.fieldNames,
      Option.apply(","), Option.apply(Array("key1", "attribute1")), Option.apply("key1 = 'TEST2'"))
    assert(appleTable.getSourceType.equals(expectedSourceType))

    val dataResult = appleTable.getDataFrame.collect()
    assert(dataResult.length == 1)

    val schemaResult = appleTable.getDataFrame.schema
    assert(schemaResult.fields.length == 2)

    assert(dataResult.take(1)(0)(schemaResult.fieldIndex("key1")).equals("\"TEST2\""))
    assert(dataResult.take(1)(0)(schemaResult.fieldIndex("attribute1")).equals("\"test number 2\""))
  }
}
