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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.reflect.TypeToken;
import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.finra.msd.helpers.FileHelper;
import org.hsqldb.server.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class MemoryDbHsql {


    private static MemoryDbHsql instance = null;
    private Server hsqlDbServer = null;

    public static final String hsqlDriverName = "org.hsqldb.jdbc.JDBCDriver";
    public static final String hsqlUrl = "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb";


    protected MemoryDbHsql() {
        // Exists only to defeat instantiation.
    }

    public static MemoryDbHsql getInstance() {
        if(instance == null) {
            instance = new MemoryDbHsql();
        }
        return instance;
    }

    public synchronized void  initializeMemoryDB() throws URISyntaxException, SQLException
    {
        if (hsqlDbServer == null)
        {
            hsqlDbServer = new Server();
            hsqlDbServer.setDatabaseName(0, "testDb");
            hsqlDbServer.setDatabasePath(0, "mem:testDb");
            hsqlDbServer.setPort(9001); // this is the default port
            hsqlDbServer.setSilent(true);
            hsqlDbServer.start();
            stageTablesAndTestData();
        }
    }

    public int getState()
    {
        if (hsqlDbServer == null)
        {
            return 0; //meaning the server is not created yet
        }
        return hsqlDbServer.getState();
    }

    private String getDataType(JsonElement element) {
        if (element.isJsonPrimitive()) {
            JsonPrimitive primitive = (JsonPrimitive) element;
            if (primitive.isBoolean())
                return "boolean";
            else if (primitive.isNumber())
                return "int";
            else
                return "varchar(255)";
        } else
            return "varchar(255)";
    }

    private String formatStringFromJson(JsonElement jsonElement) {
        if (jsonElement == null) return null;
        return jsonElement.toString().replaceAll("\\A\"(.*)\"\\z", "$1");
    }

    private void stageTablesAndTestData() throws URISyntaxException
    {
        try {
            Class.forName(hsqlDriverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try (Connection conn = DriverManager.getConnection(hsqlUrl, "SA", "")) {
            try (Statement stmt = conn.createStatement()) {
                List<String> filenamesNew = FileHelper.getFilenames("jdbc", "", ".sql").stream()
                    .sorted().collect(
                        Collectors.toList());
                filenamesNew.addAll(FileHelper.getFilenames("compare", "JsonTest", ".sql"));

                for (String filename : filenamesNew) {
                    String[] statements = FileHelper.getStringFromResource("/" + filename)
                        .split(";");
                    for (String statement : statements) {
                        stmt.execute(statement);
                    }
                }

                Set<String> filenames = FileHelper.getFilenames("dynamodb", "DynamoDbTest",
                    ".json");
                filenames.addAll(FileHelper.getFilenames("json", "JsonTest", ".json"));

                for (String filename : filenames) {
                    String tableName = filename
                        .substring(0, filename.length() - 5).replace("/", "_");
                    stmt.execute("DROP TABLE IF EXISTS " + tableName + ";");

                    String json = FileHelper.getStringFromResource("/" + filename);

                    List<JsonElement> jsonList = new Gson().fromJson(json,
                        new TypeToken<Collection<JsonElement>>() {
                        }.getType());

                    stmt.execute("CREATE TABLE " + tableName + "(\n" +
                        "           key1 varchar(255),\n" +
                        "           key2 int,\n" +
                        "           attribute1 varchar(255),\n" +
                        "           attribute2 varchar(255),\n" +
                        "           attribute3 varchar(255));");

                    for (JsonElement jsonRow : jsonList) {
                        JsonObject jsonObject = jsonRow.getAsJsonObject();
                        try (PreparedStatement preparedStatement = conn.prepareStatement(
                            "INSERT INTO " + tableName + " values(?, ?, ?, ?, ?)")) {
                            String test = jsonObject.get("attribute1").toString();
                            String key = jsonObject.get("key1").getAsString();
                            preparedStatement.setString(1, jsonObject.get("key1").getAsString());
                            preparedStatement.setInt(2, jsonObject.get("key2").getAsInt());
                            preparedStatement.setString(3,
                                formatStringFromJson(jsonObject.get("attribute1")));
                            preparedStatement.setObject(4,
                                formatStringFromJson(jsonObject.get("attribute2")));
                            preparedStatement.setString(5,
                                formatStringFromJson(jsonObject.get("attribute3")));
                            preparedStatement.executeUpdate();
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public synchronized void shutdownMemoryDb()
    {
        hsqlDbServer.shutdown();
        while (hsqlDbServer.getState() != 16)
        {
            try {
                TimeUnit.MILLISECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        hsqlDbServer = null;
    }

}
