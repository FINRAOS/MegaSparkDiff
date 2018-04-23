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
        AppleTable leftAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver",
                "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb",
                "SA",
                "",
                "(select * from Test4)", "table1");

        AppleTable rightAppleTable = SparkFactory.parallelizeJDBCSource("org.hsqldb.jdbc.JDBCDriver",
                "jdbc:hsqldb:hsql://127.0.0.1:9001/testDb",
                "SA",
                "",
                "(select * from Test5)", "table2");

        Pair<Dataset<Row>,Dataset<Row>> pair =  SparkCompare.compareAppleTables(leftAppleTable, rightAppleTable);


        //Primary Key as Java List
        List<String> primaryKey = Arrays.asList("FRUIT");
        //Convert Java List to SCALA SEQ
        Seq<String> primaryKeySeq = JavaConverters.asScalaIteratorConverter(primaryKey.iterator()).asScala().toSeq();

        String html = Visualizer.generateVisualizerTemplate(pair.getLeft(), pair.getRight(), primaryKeySeq, 100);

        System.out.println(html);
    }
}
