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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MemoryDbDynamo {

  private static MemoryDbDynamo instance = null;
  private DynamoDBProxyServer server = null;

  private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
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

  private void stageTablesAndTestData() {
    DynamoDB dynamoDB = new DynamoDB(client);

    List<Table> tables = new ArrayList<>();

    for (String tableName : Arrays.asList(
        "test_table1", "test_table1_diff",
        "test_table2", "test_table2_diff",
        "test_table3", "test_table3_diff",
        "test_table4", "test_table4_diff",
        "test_table5", "test_table5_diff",
        "test_table6", "test_table7", "test_table8",
        "test_table9", "test_table9_diff")) {
      tables.add(dynamoDB.createTable(tableName,
          Arrays.asList(new KeySchemaElement("key1", KeyType.HASH),
              new KeySchemaElement("key2", KeyType.RANGE)),
          Arrays.asList(new AttributeDefinition("key1", ScalarAttributeType.S),
              new AttributeDefinition("key2", ScalarAttributeType.N)),
          new ProvisionedThroughput(10L, 10L)));
    }

    int index = 0;

    // test_table1
    Table table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", "1")
            .with("attribute3", 1)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", "2")
            .with("attribute3", 2)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", "true")
            .with("attribute3", 3)
    );

    // test_table1_diff
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", "1")
            .with("attribute3", 1)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", "2 ")
            .with("attribute3", 2)
    );

    // test_table2
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", "1")
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", "2")
            .with("attribute3", 2)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", true)
            .with("attribute3", 3)
    );

    // test_table2_diff
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", "1")
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", "2")
            .with("attribute3", 2)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", true)
    );

    // test_table3
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", Collections.singletonList(2))
            .with("attribute3", 2)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", Arrays.asList(3, 4))
            .with("attribute3", 3)
    );

    // test_table3_diff
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", Arrays.asList(2, 3))
            .with("attribute3", 2)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", Arrays.asList(4, 3))
            .with("attribute3", 3)
    );

    // test_table4
    table = tables.get(index++);

    Set<Integer> set;

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    set = new HashSet<>();
    set.add(2);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", set)
            .with("attribute3", 2)
    );

    set = new HashSet<>();
    set.add(3);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", set)
            .with("attribute3", 3)
    );

    // test_table4_diff
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    set = new HashSet<>();
    set.add(2);
    set.add(3);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", set)
            .with("attribute3", 2)
    );

    set = new HashSet<>();
    set.add(4);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", set)
            .with("attribute3", 3)
    );

    // test_table5
    table = tables.get(index++);

    Map<String, Integer> map;

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    map = new HashMap<>();
    map.put("test", 2);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", map)
            .with("attribute3", 2)
    );

    map = new HashMap<>();
    map.put("test", 3);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", map)
            .with("attribute3", 3)
    );

    // test_table5_diff
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    map = new HashMap<>();
    map.put("test", 2);
    map.put("newtest", 2);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", map)
            .with("attribute3", 2)
    );

    map = new HashMap<>();
    map.put("test", 4);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", map)
            .with("attribute3", 3)
    );

    // test_table6
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", "1")
            .with("attribute3", 1)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", "2")
            .with("attribute3", 2)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", "true")
            .with("attribute3", 3)
            .with("attribute4", null)
    );

    // test_table7
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", Collections.singletonList(2))
            .with("attribute3", 2)
    );

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", Arrays.asList(3, 4, null))
            .with("attribute3", 3)
    );

    // test_table8
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    map = new HashMap<>();
    map.put("test", 2);
    map.put("test_add", null);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", map)
            .with("attribute3", 2)
    );

    map = new HashMap<>();
    map.put("test", 3);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", map)
            .with("attribute3", 3)
    );

    // test_table9
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    Set<Integer> set1 = new HashSet<>();
    set1.add(2);

    Set<String> set2 = new HashSet<>();
    set2.add("string 2");
    set2.add("string 2 new");

    Map<String, Object> map1 = new HashMap<>();
    map1.put("test", 2);
    map1.put("test_list", Arrays.asList(set1, set2));

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", map1)
            .with("attribute3", 2)
    );

    map1 = new HashMap<>();
    map1.put("test", 3);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", map1)
            .with("attribute3", 3)
    );

    // test_table9_diff
    table = tables.get(index++);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST1", "key2", 1)
            .with("attribute1", "test number 1")
            .with("attribute2", 1)
            .with("attribute3", 1)
    );

    set1 = new HashSet<>();
    set1.add(2);

    set2 = new HashSet<>();
    set2.add("string 2");
    set2.add("string 2 new");

    map1 = new HashMap<>();
    map1.put("test", 2);
    map1.put("test_list", Arrays.asList(set2, set1));

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST2", "key2", 1)
            .with("attribute1", "test number 2")
            .with("attribute2", map1)
            .with("attribute3", 2)
    );

    map1 = new HashMap<>();
    map1.put("test", 33);

    table.putItem(
        new Item().withPrimaryKey("key1", "TEST3", "key2", 3)
            .with("attribute1", "test number 3")
            .with("attribute2", map1)
            .with("attribute3", 3)
    );
  }

  public synchronized void shutdownMemoryDb() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }
}
