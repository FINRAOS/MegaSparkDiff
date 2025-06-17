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

package org.finra.msd.launcher;

import org.finra.msd.containers.AppleTable;
import org.finra.msd.containers.CmdLine;
import org.finra.msd.containers.SourceVars;
import org.finra.msd.sparkcompare.SparkCompare;
import org.finra.msd.sparkfactory.SparkFactory;
import scala.Option;

import java.io.IOException;

/**
 * Main class that parses the command line arguments
 * which includes source/output/configuration details, carries out comparison and provides the result of comparison
 */
public class Launcher {

    public static void main(String[] args) throws IOException
    {
        /** Parse **/
        CmdLine values = new CmdLine(args);
        SourceVars sv1 = new SourceVars(values.getSource1()),
                   sv2 = new SourceVars(values.getSource2());

        /** Generate AppleTables from inputs **/
        SparkFactory.initializeSparkContext();
        AppleTable leftAppleTable = generateAppleTable(sv1, values.getData1(),"table1");
        AppleTable rightAppleTable = generateAppleTable(sv2, values.getData2(),"table2");


        /** Compare tables and save output to file **/
        SparkCompare.compareAppleTablesSaveResults(
                leftAppleTable,
                rightAppleTable,
                values.getOutputDirectory(),
                true , values.getDelimiter());
        SparkFactory.stopSparkContext();
    }


    /**
     * Generate the table (in our internal format) containing the data specified by the input
     * @param sv Source Variables
     * @param dataSetName the parameter name representing the designated dataset
     * @param tempViewName a user specified name for this view
     * @return
     */
    public static AppleTable generateAppleTable(SourceVars sv, String dataSetName, String tempViewName)
    {
        switch (sv.getConnection().toLowerCase())
        {
            case "jdbc": return SparkFactory.parallelizeJDBCSource(
                                    sv.getVar("driver"),
                                    sv.getVar("url"),
                                    sv.getVar("user"),
                                    sv.getVar("password"),
                                    sv.getQuery(dataSetName),
                                    tempViewName,
                                    Option.apply(sv.getVar("delimiter")),
                                    Option.apply(sv.getVar("iamAuth")),
                                    Option.apply(sv.getVar("region")));
            case "hive": return SparkFactory.parallelizeHiveSource(
                                    sv.getQuery(dataSetName),
                                    tempViewName);
            case "file": return SparkFactory.parallelizeTextSource(
                                    sv.getQuery(dataSetName),
                                    tempViewName);
            default: return null;
        }
    }
}
