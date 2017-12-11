package org.finra.msd.containers;

import org.finra.msd.util.FileUtil;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

/**
 * Extracts the database connection details
 */
public class SourceVars
{
    /**
     * Specifies source type
     */
    private String connection;

    /**
     * Connection variables
     */
    private Map<String, String> vars;

    /**
     * Queries to retrieve data
     */
    private Map<String, String> queries;

    /**
     * Initializes the constructor with a file consisting database connection details
     * @param fileName
     */
    public SourceVars(String fileName)
    {
        this(new File(fileName));
    }

    /**
     * Convert an input configuration file into a map
     * containing connection properties and query specifications
     * @param source this is the input file that contains the configuration per the spec in the usage documentation
     */
    public SourceVars(File source)
    {
        vars = new HashMap<String, String>();
        queries = new HashMap<String, String>();

        List<String> fileContents = FileUtil.fileToStringList(source);
        for (String line : fileContents)
        {
            line = line.trim().replaceAll(" +"," ");
            if (line.isEmpty())
                continue;
            if (line.charAt(0) == '@')
            {
                String dataName = line.substring(1,line.indexOf(":")).trim(),
                       query = line.substring(line.indexOf(":")+1).trim();
                queries.put(dataName,query);
            }
            else if (line.contains("=") && line.indexOf("=") == line.lastIndexOf("="))
            {
                String key = line.substring(0,line.indexOf("=")).trim(),
                       val = line.substring(line.indexOf("=")+1).trim();
                if (key.equals("connection"))
                    connection = val;
                else
                    vars.put(key,val);
            }
        }
    }


    /**
     * Gets the type of database connection
     * @return This string represents the full JDBC connection that will be passed to the JDBC driver in order
     * to connect to the DB. The expectation is that it contains the authentication user and password as well.
     */
    public String getConnection() { return connection; }

    /**
     * Gets the database connection details based on the key provided
     * @param key Variable used to define the connection details like driver, URL, username, password
     * @return The value of the passed variable for database connection
     */
    public String getVar(String key) { return vars.get(key); }

    /**
     * Gets the data retrieval query specified in the source file based on the corresponding variable name
     * @param dataName variable to describe the source
     * @return query associated with the passed variable name
     */
    public String getQuery(String dataName) { return queries.get(dataName); }
}
