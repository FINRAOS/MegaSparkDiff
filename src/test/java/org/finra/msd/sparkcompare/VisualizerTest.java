package org.finra.msd.sparkcompare;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.finra.msd.containers.AppleTable;
import org.finra.msd.sparkcompare.baseclasses.BaseJunitForSparkCompare;
import org.finra.msd.sparkfactory.SparkFactory;
import org.finra.msd.visualization.Visualizer;
import org.junit.Assert;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

public class VisualizerTest extends BaseJunitForSparkCompare {

    @Test
    public void basicVisualizerTest()
    {
        Pair<Dataset<Row>,Dataset<Row>> pair = getAppleTablePair("Test6", "Test7");
        String html = generateString(pair.getLeft(), pair.getRight(), "FRUIT", 100);
        if (html.isEmpty())
        {
            Assert.fail("html was empty");
        }
    }

    @Test
    public void emptyLeftDfTest()
    {
        Pair<Dataset<Row>,Dataset<Row>> pair = getAppleTablePair("Test4", "Test1");
        String html = generateString(pair.getLeft(), pair.getRight(), "FRUIT", 100);
        if (html.isEmpty())
        {
            Assert.fail("html was empty");
        }
    }

    @Test
    public void emptyRightDfTest()
    {
        Pair<Dataset<Row>,Dataset<Row>> pair = getAppleTablePair("Test1", "Test4");
        String html = generateString(pair.getLeft(), pair.getRight(), "FRUIT", 100);
        if (html.isEmpty())
        {
            Assert.fail("html was empty");
        }
    }

    @Test
    public void nullLeftDfTest()
    {
        Pair<Dataset<Row>,Dataset<Row>> pair = getAppleTablePair("Test1", "Test4");
        String html = generateString(null, pair.getRight(), "FRUIT", 100);
        Assert.assertEquals("<h3>Error message: Left dataframe is null</h3>", html);
    }

    @Test
    public void nullRightDfTest()
    {
        Pair<Dataset<Row>,Dataset<Row>> pair = getAppleTablePair("Test1", "Test4");
        String html = generateString(pair.getLeft(), null, "FRUIT", 100);
        Assert.assertEquals("<h3>Error message: Right dataframe is null</h3>", html);
    }

    @Test
    public void emptyKeyTest()
    {
        Pair<Dataset<Row>,Dataset<Row>> pair = getAppleTablePair("Test1", "Test4");
        String html = generateString(pair.getLeft(), pair.getRight(), "", 100);
        Assert.assertEquals("<h3>Error message: One or more keys is empty or null</h3>", html);
    }

    @Test
    public void nullKeyTest()
    {
        Pair<Dataset<Row>,Dataset<Row>> pair = getAppleTablePair("Test1", "Test4");
        String html = generateString(pair.getLeft(), pair.getRight(), null, 100);
        Assert.assertEquals("<h3>Error message: One or more keys is empty or null</h3>", html);
    }

    @Test
    public void keyCaseTest()
    {
        Pair<Dataset<Row>,Dataset<Row>> pair = getAppleTablePair("Test1", "Test4");
        boolean flag = true;
        String result1 = "";
        String result2 = "";

        try{
            result1 = generateString(pair.getLeft(), pair.getRight(), "Fruit", 100);
            result2 = generateString(pair.getLeft(), pair.getRight(), "FrUit", 100);
        } catch (Exception ex) {
            flag = false;
        }

        Assert.assertEquals(true, flag);
        Assert.assertEquals(result1, result2);
    }

    @Test
    public void invalidMaxRecordsTest()
    {
        Pair<Dataset<Row>,Dataset<Row>> pair = getAppleTablePair("Test1", "Test4");
        boolean flag = true;

        try{
            generateString(pair.getLeft(), pair.getRight(), "FRUIT", -100);
        } catch(Exception ex) {
            flag = false;
        }

        Assert.assertEquals(true, flag);
    }

    private String generateString(Dataset<Row> left, Dataset<Row> right, String key, int maxRecords){
        //Primary Key as Java List
        List<String> primaryKey = Arrays.asList(key);
        //Convert Java List to SCALA SEQ
        Seq<String> primaryKeySeq = JavaConverters.asScalaIteratorConverter(primaryKey.iterator()).asScala().toSeq();

        String html = Visualizer.generateVisualizerTemplate(left, right, primaryKeySeq, maxRecords);

        return html;
    }

    private Pair<Dataset<Row>,Dataset<Row>> getAppleTablePair(String testName1, String testName2){
        AppleTable leftAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver",
                "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb",
                "SA",
                "",
                "(select * from " + testName1 + ")", "table1");

        AppleTable rightAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver",
                "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb",
                "SA",
                "",
                "(select * from " + testName2 + ")", "table2");

        return SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable);
    }
}

