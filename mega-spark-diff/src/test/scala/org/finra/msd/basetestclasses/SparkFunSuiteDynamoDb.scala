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

import org.finra.msd.memorydb.MemoryDbDynamo

class SparkFunSuiteDynamoDb
  extends SparkFunSuite {


  protected final val dynamoDbEndpoint = "http://localhost:8000"
  protected final val dynamoDbCustomAWSCredentialsProvider = "com.amazonaws.auth.SystemPropertiesCredentialsProvider"

  override def beforeAll(): Unit = synchronized {
    super.beforeAll()

    MemoryDbDynamo.getInstance().initializeMemoryDb()

    System.setProperty("aws.dynamodb.endpoint", dynamoDbEndpoint)

    System.setProperty("aws.accessKeyId", "test")
    System.setProperty("aws.secretKey", "test")
  }


  override def afterAll(): Unit = {
    super.afterAll()

    MemoryDbDynamo.getInstance().shutdownMemoryDb()
  }
}