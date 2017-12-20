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

import org.hsqldb.server.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MemoryDbHsql {


    private static MemoryDbHsql instance = null;
    Server hsqlDbServer;


    protected MemoryDbHsql() {
        // Exists only to defeat instantiation.
    }

    public static MemoryDbHsql getInstance() {
        if(instance == null) {
            instance = new MemoryDbHsql();
        }
        return instance;
    }

    public void initializeMemoryDB()
    {
        hsqlDbServer = new Server();
        hsqlDbServer.setDatabaseName(0, "testDb");
        hsqlDbServer.setDatabasePath(0, "mem:testDb");
        hsqlDbServer.setPort(9001); // this is the default port
        hsqlDbServer.start();
    }

    public int getState()
    {
        return hsqlDbServer.getState();
    }

    public void stageTablesAndTestData()
    {
        String url="jdbc:hsqldb:hsql://127.0.0.1:9001/testDb";
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, "SA", "");
            Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE Persons1 (\n" +
                    "           PersonID int,\n" +
                    "           LastName varchar(255),\n" +
                    "           FirstName varchar(255),\n" +
                    "           Address varchar(255),\n" +
                    "           City varchar(255));");
            String[] fruitTables = {"Test1","Test2","Test3","Test4","Test5"};
            for (String fruitTable : fruitTables)
                stmt.execute("CREATE TABLE " + fruitTable + " (\n" +
                        "           Fruit VARCHAR(255),\n" +
                        "           Price INT,\n" +
                        "           Ripeness INT,\n" +
                        "           Color VARCHAR(255));");

            String[] personRecords = {
                    "insert into Persons1 values(1,'Garcia', 'Carlos', 'lives somewhere', 'Rockville') ",
                    "insert into Persons1 values(2,'Patel', 'Shraddha', 'lives somewhere', 'Maryland') ",

                    "insert into Test1 values('Apple', 5, 10, 'Red') ",
                    "insert into Test1 values('Banana', 4, 8, 'Yellow') ",
                    "insert into Test1 values('Orange', 2, 9, 'Blue') ",
                    "insert into Test1 values('Kiwi', 8, 7, 'Fuzzy-Green') ",
                    "insert into Test1 values('Watermelon', 3, 11, 'Green') ",
                    "insert into Test1 values('Mango', 6, 12, 'Yellow') ",
                    "insert into Test1 values('Papaya', 190534, 4, 'I forget') ",
                    "insert into Test1 values('Strawberry', 5, 10, 'Acne') ",
                    "insert into Test1 values('Plum', 8261, 6, 'Purple') ",
                    "insert into Test1 values('Tomato', 0, 0, 'Red') ",

                    "insert into Test2 values('Apple', 5, 10, 'Red') ",
                    "insert into Test2 values('Banana', 4, 8, 'Yellow') ",
                    "insert into Test2 values('Orange', 2, 9, 'Blue') ",
                    "insert into Test2 values('Kiwi', 8, 7, 'Fuzzy-Green') ",
                    "insert into Test2 values('Watermelon', 3, 11, 'Green') ",
                    "insert into Test2 values('Mango', 6, 12, 'Yellow') ",
                    "insert into Test2 values('Papaya', 190534, 4, 'I forget') ",
                    "insert into Test2 values('Strawberry', 5, 10, 'Acne') ",
                    "insert into Test2 values('Plum', 8261, 6, 'Purple') ",
                    "insert into Test2 values('Tomato', 0, 0, 'Red') ",

                    "insert into Test3 values('Apple', 5, 10, 'Red') ",
                    "insert into Test3 values('Banana', 4, 8, 'Yellow') ",
                    "insert into Test3 values('Orange', 2, -9, 'Blue') ", //diff
                    "insert into Test3 values('Kiwi', 8, 7, 'Fuzzy-Green') ",
                    "insert into Test3 values('Watermelon', 3, 11, 'Green') ",
                    "insert into Test3 values('Mango', 6, 12, 'Yellow') ",
                    "insert into Test3 values('Papaya', 190534, 4, 'I remember now') ", //diff
                    "insert into Test3 values('Strawberry', 5, 10, 'Acne') ",
                    "insert into Test3 values('Plum', 8261, 6, 'Purple') ",
                    "insert into Test3 values('Tomato', 0, 0, 'Red') ",

                    "insert into Test4 values('Apple', 5, 10, 'Red') ",
                    "insert into Test4 values('Banana', 4, 8, 'Yellow') ",
                    "insert into Test4 values('Orange', 2, 9, 'Blue') ",
                    "insert into Test4 values('Kiwi', 8, 7, 'Fuzzy-Green') ",
                    "insert into Test4 values('Watermelon', 3, 11, 'Green') ",

                    "insert into Test5 values('Mango', 6, 12, 'Yellow') ",
                    "insert into Test5 values('Papaya', 190534, 4, 'I forget') ",
                    "insert into Test5 values('Strawberry', 5, 10, 'Acne') ",
                    "insert into Test5 values('Plum', 8261, 6, 'Purple') ",
                    "insert into Test5 values('Tomato', 0, 0, 'Red') "
            };
            for (String record : personRecords)
                stmt.executeUpdate(record);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void shutdownMemoryDb()
    {
        hsqlDbServer.stop();
    }

}
