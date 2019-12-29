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

package org.finra.msd.containers;
import org.finra.msd.enums.SourceType;
import org.finra.msd.util.FileUtil;

import java.io.File;
import java.util.List;

/**
 * Runner class parsing command line arguments
 */
public class CmdLine
{
    /**
     * Location of source data
     */
    private String dataSourceFolder;
    /**
     * Location of output
     */
    private String outputDirectory;
    /**
     * Name of configuration file
     */
    private String runConfigName;

    /**
     * Name of the data retrieval query 1
     */
    private String data1;

    /**
     * Name of the data retrieval query 2
     */
    private String data2;

    /**
     * File name where the data retrieval queries can be found
     */
    private File source1;

    /**
     * File name where the data retrieval queries can be found
     */
    private File source2;

    /**
     * Source type 1
     */
    private SourceType type1;

    /**
     * Source type 2
     */
    private SourceType type2;

    /**
     * Execution mode
     */
    private boolean local;

    /**
     * Cluster id
     */
    private String clusterId;

    /**
     * Cluster name
     */
    private String clusterName;

    /**
     * Number of executors
     */
    private String numExecutors;

    /**
     * Executor memory
     */
    private String executorMemory;

    /**
     * Controller RAM
     */
    private String controllerRAM;

    /**
     * Numbe rof partitions
     */
    private String numPartitions;

    /**
     * Partition column name
     */
    private String partitionByColumn;

    /**
     * Lower bound in case of partitioning
     */
    private String lowerBound;

    /**
     * Upper bound in case of partitioning
     */
    private String upperBound;

    private String delimiter = ",";

    /**
     * Gets the location of source data
     * @return
     */
    public String getDataSourceFolder() { return dataSourceFolder; }

    /**
     * Gets location of output directory
     * @return
     */
    public String getOutputDirectory() { return outputDirectory; }

    /**
     * Gets the file name of the configuration file
     * @return
     */
    public String getRunConfigName() { return runConfigName; }

    /**
     * Gets the data retrieval query for source 1
     * @return
     */
    public String getData1() { return data1; }

    /**
     * Gets the data retrieval query for source 2
     * @return
     */
    public String getData2() { return data2; }

    /**
     * Gets the file where the data retrieval queries can be found
     * @return
     */
    public File getSource1() { return source1; }

    /**
     * Gets the file where the data retrieval queries can be found
     * @return
     */
    public File getSource2() { return source2; }

    /**
     * Gets the type of source 1
     * @return
     */
    public SourceType getType1() { return type1; }

    /**
     * Gets the type of source 2
     * @return
     */
    public SourceType getType2() { return type2; }

    /**
     * Returns true if the execution mode is local
     * @return
     */
    public boolean isLocal() { return local; }

    /**
     * Gets the cluster Id if the execution mode is EMR
     * @return
     */
    public String getClusterId() { return clusterId; }

    /**
     * Gets the cluster name is the execution mode is EMR
     * @return
     */
    public String getClusterName() { return clusterName; }

    /**
     * Gets the number of executors passed in the configuration file
     * @return
     */
    public String getNumExecutors() { return numExecutors; }

    /**
     * Gets executor memory passed in the configuration file
     * @return
     */
    public String getExecutorMemory() { return executorMemory; }

    /**
     * Gets controller RAM passed in the configuration file
     * @return
     */
    public String getControllerRAM() { return controllerRAM; }

    /**
     * Gets the number of partitions passed in the configuration file
     * @return
     */
    public String getNumPartitions() { return numPartitions; }

    /**
     * Gets the the partition column name
     * @return
     */
    public String getPartitionByColumn() { return partitionByColumn; }

    /**
     * Gets the lower bound in case of partitioning
     * @return
     */
    public String getLowerBound() { return lowerBound; }

    /**
     * Gets the upper bound in case of partitioning
     * @return
     */
    public String getUpperBound() { return upperBound; }


    public String getDelimiter() {
        return delimiter;
    }

    /**
     * Parse the command line to retrieve at least the necessary parameters
     * @param args Input/Output/Query parameters
     */
    public CmdLine(String[] args)
    {
        // Required input/output parameters
        dataSourceFolder = "";
        outputDirectory = "";
        runConfigName = "";

        // Required query names
        data1 = "";
        data2 = "";

        for (int i=0; i<args.length; i++)
        {
            String cur = args[i];

            // Parse query names
            if (cur.startsWith("--"))
            {
                if (data1.isEmpty())
                    data1 = cur.substring(2);
                else if (data2.isEmpty())
                    data2 = cur.substring(2);
            }

            // Parse other settings
            else if (cur.contains("=") && cur.indexOf("=") == cur.lastIndexOf("="))
            {
                String before = cur.substring(0, cur.indexOf("=")),
                        after = cur.substring(cur.indexOf("=") + 1);
                switch (before)
                {
                    // Input/output
                    case "-ds": dataSourceFolder = after; break;
                    case "-od": outputDirectory = after; break;
                    case "-rc": runConfigName = after; break;

                    // Local or cluster run
                    case "-local": local = Boolean.parseBoolean(after); break;
                    case "-ci":
                    case "-cluster-id": clusterId = after; break;
                    case "-cn":
                    case "-cluster-name": clusterName = after; break;
                    case "-delimiter": delimiter = after; break;

                    default: break;
                }
            }
        }

        // Parse config file if one was specified
        if (!runConfigName.isEmpty())
            parseRunConfig();

        // Error out if all required parameters were not provided
        if (dataSourceFolder.isEmpty() || outputDirectory.isEmpty() || data1.isEmpty() || data2.isEmpty()) // ERROR
        {
            System.out.println("dataSourceFolder: " + dataSourceFolder);
            System.out.println("outputDirectory:  " + outputDirectory);
            System.out.println("data1:            " + data1);
            System.out.println("data2:            " + data2);
            System.out.println("must all be populated.");
            System.exit(1);
        }

        // Discover which config files house the specified queries
        findSourceFile(data1);
        findSourceFile(data2);
    }

    /**
     * Parses a run configuration file containing Spark specific parameters
     */
    private void parseRunConfig()
    {
        File runConfigFile = new File(runConfigName);
        List<String> runConfigs = FileUtil.fileToStringList(runConfigFile);
        for (String cur : runConfigs)
            if (cur.contains("=") && cur.indexOf("=") == cur.lastIndexOf("="))
            {
                String before = cur.substring(0,cur.indexOf("=")),
                        after = cur.substring(cur.indexOf("=")+1);
                switch (before)
                {
                    // Spark parameters
                    case "numExecutors": numExecutors = after; break;
                    case "executorMemory": executorMemory = after; break;
                    case "controllerRAM": controllerRAM = after; break;
                    case "numPartitions": numPartitions = after; break;

                    // Partition parameters
                    case "partitionByColumn": partitionByColumn = after; break;
                    case "lowerBound": lowerBound = after; break;
                    case "upperBound": upperBound = after; break;
                    default: break;
                }
            }
    }

    /**
     * Discovers the source file and source type based on the query passed
     * @param dataName Query
     */
    private void findSourceFile(String dataName)
    {
        File cmdFile = new File(dataSourceFolder);
        File files[] = cmdFile.listFiles();
        for (File file : files)
        {
            SourceType foundType = fileContains(file, dataName);
            if (foundType != null)
            {
                if (dataName.equals(data1))
                {
                    source1 = file;
                    type1 = foundType;
                }
                else // table2
                {
                    source2 = file;
                    type2 = foundType;
                }
                return;
            }
        }
    }

    /**
     * Returns the source type if the file contains a query
     * @param file File Name
     * @param dataName Query
     * @return
     */
    public static SourceType fileContains(File file, String dataName)
    {
        SourceVars source = new SourceVars(file);
        if (source.getQuery(dataName) != null)
            return SourceType.valueOf(source.getConnection().toUpperCase());
        else
            return null;
    }
}
