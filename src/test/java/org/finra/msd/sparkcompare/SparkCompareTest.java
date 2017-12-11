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

package org.finra.msd.sparkcompare;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.finra.msd.containers.AppleTable;
import org.finra.msd.sparkcompare.baseclasses.BaseJunitForSparkCompare;
import org.finra.msd.sparkfactory.SparkFactory;
import org.finra.msd.util.FileUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class SparkCompareTest extends BaseJunitForSparkCompare {

    private static final Logger log = Logger.getLogger(SparkCompareTest.class.getName());


    public SparkCompareTest() {
    }


    /**
     * Test of compareRdd method, of class SparkCompare.
     */
    @Test
    public void testCompareRdd() {
       
        //code to get file1 location
        String file1Path = this.getClass().getClassLoader().
                getResource("TC5NullsAndEmptyData1.txt").getPath();
        
        String file2Path = this.getClass().getClassLoader().
                getResource("TC5NullsAndEmptyData2.txt").getPath();

        Pair<Dataset<Row>, Dataset<Row>> comparisonResult = SparkCompare.compareFiles(file1Path, file2Path);

        try {
            comparisonResult.getLeft().show();
            comparisonResult.getRight().show();
        } catch (Exception e) {
            Assert.fail("Straightforward output of test results somehow failed");
        }
    }

    @Test
    public void testCompareFileSaveResults()
    {
        String file1Path = this.getClass().getClassLoader().
                getResource("Test4.txt").getPath();

        String file2Path = this.getClass().getClassLoader().
                getResource("Test6.txt").getPath();

        String testLoc = "file_test";
        cleanOutputDirectory("/" + testLoc);
        SparkCompare.compareFileSaveResults(file1Path,file2Path,outputDirectory + "/" + testLoc, true);

        File outputFile = new File(outputDirectory + "/" + testLoc);
        if (!outputFile.exists())
            Assert.fail("Failed to write to output file");
    }

    @Test
    public void testCompareAppleTables()
    {
        AppleTable appleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver",
                "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb",
                "SA",
                "",
                "(select * from Persons1)", "table1");

        Pair<Dataset<Row>, Dataset<Row>> pair = SparkCompare.compareAppleTables(appleTable, appleTable);
        //the expectation is that there is no diffrence
        if (pair.getLeft().count() != 0)
        {
            Assert.fail("found different records in Left dataframe although passed in the same table as input");
        }
        if (pair.getRight().count() != 0)
        {
            Assert.fail("found different records in right dataFrame although passed in the same table as input");
        }
    }

    @Test
    public void testCompareJDBCAppleTablesWithDifference()
    {
        AppleTable leftAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver",
                "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb",
                "SA",
                "",
                "(select * from Persons1 where personid=2)", "table1");


        AppleTable rightAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver",
                "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb",
                "SA",
                "",
                "(select * from Persons1 Where personid=1)", "table2");

        Pair<Dataset<Row>, Dataset<Row>> pair = SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable);

        //the expectation is one difference
        if (pair.getLeft().count() != 1)
        {
            Assert.fail("expected 1 different record in left");
        }
        if (pair.getRight().count() != 1)
        {
            Assert.fail("expected 1 different record in right");
        }
    }


    @Test
    public void testCompareJDBCtpFileAppleTablesWithDifference()
    {
        AppleTable leftAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver",
                "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb",
                "SA",
                "",
                "(select * from Persons1)", "table1");

        String file1Path = this.getClass().getClassLoader().
                getResource("TC1DiffsAndDups1.txt").getPath();

        AppleTable rightAppleTable = SparkFactory.parallelizeTextSource(file1Path,"table2");

        Pair<Dataset<Row>, Dataset<Row>> pair = SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable);

        //the expectation is one difference
        if (pair.getLeft().count() != 2)
        {
            Assert.fail("expected 2 different record in left");
        }
        if (pair.getRight().count() != 5)
        {
            Assert.fail("expected 5 different record in right");
        }
    }

    @Test
    public void testCompareJDBCtpFileAppleTablesWithDifferenceAndSaveToFile()
    {
        AppleTable leftAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver",
                "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb",
                "SA",
                "",
                "(select * from Persons1)", "table1");

        String file1Path = this.getClass().getClassLoader().
                getResource("TC1DiffsAndDups1.txt").getPath();

        String testLoc = "jdbc_test";
        AppleTable rightAppleTable = SparkFactory.parallelizeTextSource(file1Path,"table2");
        cleanOutputDirectory("/" + testLoc);
        SparkCompare.compareAppleTablesSaveResults(leftAppleTable, rightAppleTable
                , outputDirectory + "/" + testLoc, true);

        File outputFile = new File(outputDirectory + "/" + testLoc);
        if (!outputFile.exists())
            Assert.fail("Failed to write to output file");
    }


    private void cleanOutputDirectory(String subdir)
    {
        //will clean the output directory
        try {
            FileUtils.deleteDirectory(new File(outputDirectory + subdir));
        } catch (IOException e) {
            log.severe("couldnt delete test output from prior run");
            log.severe(e.toString());
        }
        FileUtil.createDirectory(outputDirectory + subdir);
    }
}
