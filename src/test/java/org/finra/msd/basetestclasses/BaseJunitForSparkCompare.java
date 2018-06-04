package org.finra.msd.basetestclasses;

import org.finra.msd.memorydb.MemoryDbHsql;
import org.finra.msd.sparkfactory.SparkFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseJunitForSparkCompare {

    protected final String outputDirectory = System.getProperty("user.dir") + "/sparkOutputDirectory";

    @BeforeClass
    public static void setUpClass() {
        SparkFactory.initializeSparkLocalMode("local[*]" , "WARN" ,"1");

        if (MemoryDbHsql.getInstance().getState() != 1 )
        {
            MemoryDbHsql.getInstance().initializeMemoryDB();
            MemoryDbHsql.getInstance().stageTablesAndTestData();
        }
    }

    @AfterClass
    public static void tearDownClass() {
        SparkFactory.stopSparkContext();
        //TODO: need to implement a better way to close in memory DB
        //MemoryDbHsql.getInstance().shutdownMemoryDb();
    }
}
