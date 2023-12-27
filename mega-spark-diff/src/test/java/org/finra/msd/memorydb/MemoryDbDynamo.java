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

package org.finra.msd.memorydb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.finra.msd.helpers.FileHelper;

public class MemoryDbDynamo {

  private static MemoryDbDynamo instance = null;
  private DynamoDBProxyServer server = null;

  private static final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(new AWSCredentialsProvider() {
        @Override
        public AWSCredentials getCredentials() {
          return new BasicAWSCredentials("test", "test");
        }

        @Override
        public void refresh() {

        }
      })
      .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", null))
      .build();

  protected MemoryDbDynamo() {
    // Exists to prevent external instantiation.
  }

  public static MemoryDbDynamo getInstance() {
    if (instance == null) {
      instance = new MemoryDbDynamo();
    }
    return instance;
  }

  public synchronized void initializeMemoryDb() throws Exception {
    if (server == null) {
      System.setProperty("sqlite4java.library.path", "target/testDependencies");

      String[] localArgs = {"-inMemory", "-port", "8000"};
      server = ServerRunner.createServerFromCommandLineArgs(localArgs);
      server.start();

      stageTablesAndTestData();
    }
  }

  private void stageTablesAndTestData() throws URISyntaxException {
    DynamoDB dynamoDB = new DynamoDB(client);

    Map<String, Table> tables = new HashMap<>();

    Set<String> filenames = FileHelper.getFilenames("dynamodb", "DynamoDbTest", ".json");
    filenames.addAll(FileHelper.getFilenames("json", "JsonTest", ".json"));
    filenames.addAll(FileHelper.getFilenames("compare", "JsonTest", ".json"));

    for (String filename : filenames) {
      String tableName = filename
          .substring(0, filename.length() - 5);
      tables.put(filename,
          dynamoDB.createTable(tableName.replace("/", "_"),
          Arrays.asList(new KeySchemaElement("key1", KeyType.HASH),
              new KeySchemaElement("key2", KeyType.RANGE)),
          Arrays.asList(new AttributeDefinition("key1", ScalarAttributeType.S),
              new AttributeDefinition("key2", ScalarAttributeType.N)),
          new ProvisionedThroughput(10L, 10L)));
    }

    for (Map.Entry<String, Table> entry : tables.entrySet()) {
      Table table = entry.getValue();
      String json = FileHelper.getStringFromResource("/" + entry.getKey());

      List<JsonElement> jsonList = new Gson().fromJson(json,
          new TypeToken<Collection<JsonElement>>() {
          }.getType());

      for (JsonElement jsonRow : jsonList) {
        Item jsonItem = Item.fromJSON(jsonRow.toString());
        jsonItem = jsonItem.withPrimaryKey("key1", jsonItem.get("key1"), "key2",
            Integer.valueOf(String.valueOf(jsonItem.get("key2"))));

        if (table.getTableName().startsWith("dynamodb_DynamoDbTestSet")) {
          Object attribute2 = jsonRow.getAsJsonObject().get("attribute2");
          if (attribute2 instanceof JsonArray) {
            JsonArray array = (JsonArray) attribute2;
            Set<Object> set = new HashSet<>();
            for (int i = 0; i < array.size(); i++) {
              JsonPrimitive element = array.get(i).getAsJsonPrimitive();
              set.add(element.isNumber() ? element.getAsNumber() : element.getAsString());
            }
            jsonItem = jsonItem.with("attribute2", set);
          }
        }

        table.putItem(jsonItem);
      }
    }
  }

  public synchronized void shutdownMemoryDb() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }
}
