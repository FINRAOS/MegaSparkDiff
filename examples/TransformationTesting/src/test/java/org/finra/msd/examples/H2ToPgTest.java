package org.finra.msd.examples;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.finra.msd.containers.AppleTable;
import org.finra.msd.enums.SourceType;
import org.finra.msd.examples.db.H2Database;
import org.finra.msd.examples.db.PostgresDatabase;
import org.finra.msd.sparkcompare.SparkCompare;
import org.finra.msd.sparkfactory.SparkFactory;
import org.junit.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;

public class H2ToPgTest {

  @BeforeClass
  public static void start() throws IOException {
    PostgresDatabase.startPostgres();
  }

  @AfterClass
  public static void stop() throws IOException {
    PostgresDatabase.stopPostgres();
  }

  @Before
  public void setUp() throws IOException, SQLException {
    PostgresDatabase.setUp();
    H2Database.setUp();
  }

  @After
  public void tearDown() throws IOException, SQLException {
    PostgresDatabase.tearDown();
    H2Database.tearDown();
  }

  @Test
  public void testTransformH2ToPg() throws SQLException {
    SparkFactory.initializeSparkLocalMode("local[*]", "WARN", "1");

    // Shown below are the two methods of reading a JDBC database.
    // The first uses a pre-configured method that directly gives you an AppleTable.
    // The second builds a customized Spark RDD and converts it into an AppleTable.
    // The first is nice for quick setups, and basic applications.
    // The second is nearly required for any performance enhancements, and is highly recommended.
    // Parallelize the source data using pre-configured method.
    AppleTable leftTable = SparkFactory
      .parallelizeJDBCSource("org.h2.Driver",
        H2Database.getUrl(),
        H2Database.getProperties().getProperty("user"),
        H2Database.getProperties().getProperty("password"),
        "(select * from appliance) a", "appliance_left");

    // Parallelize the target data using a customized Spark RDD.
    // See the below link to find out what these options do.
    // It's recommended to use the below settings as a bare minimum.
    // http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
    Dataset<Row> rightDataFrame = SparkFactory.sparkSession().sqlContext().read()
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", PostgresDatabase.getUrl())
      .option("dbtable", "(select * from appliance) a")
      .option("user", PostgresDatabase.getProperties().getProperty("user"))
      .option("password", PostgresDatabase.getProperties().getProperty("password"))
      .option("partitionColumn", "price") // A numeric column
      .option("lowerBound", "0") // Typically you want this to be the minimum value
      .option("upperBound", "500") // Typically you want this to be the maximum value
      .option("numPartitions", "2") // Number of partitions to break the db into
      .option("fetchSize", "10") // Default is 10, increasing reduces network lag
      .load();

    rightDataFrame.createOrReplaceTempView("appliance_right");

    AppleTable rightTable = new AppleTable(SourceType.JDBC, rightDataFrame, ",", "appliance_right");

    // Parallelize the reference data
    AppleTable typeTable = SparkFactory
        .parallelizeJDBCSource("org.postgresql.Driver",
            PostgresDatabase.getUrl(),
            PostgresDatabase.getProperties().getProperty("user"),
            PostgresDatabase.getProperties().getProperty("password"),
            "(select * from appliance_type) a", "appliance_type");
    
    // Handle the source's "NAME" column transformation/split to the target's "name" and "brand"
    // columns.
    // First register two new UDFs, giving the UDFs a name, a lambda, and a data type to return.
    // We use the WordUtils.capitalize method of the Apache Commons Lang library to capitalize
    // the first character of each word.
    // Both UDFs return a String type, since the column data type is the same among both tables.

    // For the target name, the lambda gets the data before the comma.
    SparkFactory.sparkSession().udf().register("split_name",
        (String x) -> WordUtils.capitalize(x.substring(0, x.indexOf(","))),
        DataTypes.StringType);

    // And for the target brand, the lambda gets the data after the comma.
    SparkFactory.sparkSession().udf().register("split_brand",
        (String x) -> WordUtils.capitalize(x.substring(x.indexOf(",") + 1)),
        DataTypes.StringType);


    // Handle the round-up integer division of "SALES_AMOUNT" and "PRICE" to determine units_sold.
    SparkFactory.sparkSession().udf().register("calculate_units_sold",
        (BigDecimal x, BigDecimal y) -> Integer.valueOf(
            x.divide(y, BigDecimal.ROUND_HALF_UP).setScale(0, BigDecimal.ROUND_HALF_UP).toString()
        ),
        DataTypes.IntegerType);

    // Handle the capitalization the first letter of each word of the source's "TYPE".
    SparkFactory.sparkSession().udf().register("capitalize_type",
        (String x) -> WordUtils.capitalize(x),
        DataTypes.StringType);




    Dataset<Row> leftTableTransformDF = leftTable.getDataFrame();

    // Lower case all the columns in the source dataframe.
    for (String column : leftTableTransformDF.columns()) {
      leftTableTransformDF = leftTableTransformDF.withColumnRenamed(column, column.toLowerCase());
    }

    // Call withColumn operations, passing the source "NAME" to the UDFs "split_name" and
    // "split_brand" and storing the results in "name_temp" and "brand" respectively.
    // Then drop column "name" and rename "name_temp" to "name".
    leftTableTransformDF = leftTableTransformDF
        .withColumn("name_temp",
            functions.callUDF("split_name", functions.col("name")))
        .withColumn("brand",
            functions.callUDF("split_brand", functions.col("name")))
        .drop("name")
        .withColumnRenamed("name_temp", "name");

    // Call the withColumn operation, passing both the source SALES_AMOUNT and PRICE columns to the
    // UDF "calculate_units_sold" and storing the result in column "units_sold".
    leftTableTransformDF = leftTableTransformDF
        .withColumn("units_sold",
            functions.callUDF("calculate_units_sold", functions.col("sales_amount"),
                functions.col("price")));

    // Call the withColumn operation, passing the "TYPE" column to the UDF "capitalize_type" and
    // storing the result in column "type".
    leftTableTransformDF = leftTableTransformDF
        .withColumn("type",
            functions.callUDF("capitalize_type", functions.col("type")));

    // Join with the reference table to get the "type_id" value, drop the original "type" column,
    // and rename "type_id" to "type".
    leftTableTransformDF = leftTableTransformDF.as("a").join(typeTable.getDataFrame().as("b"),
        leftTableTransformDF.col("type").equalTo(typeTable.getDataFrame().col("type_name")),
        "leftouter")
        .select("a.*", "b.type_id")
        .drop("type")
        .withColumnRenamed("type_id", "type");




    // Select all columns in transformed left dataframe that exist in right dataframe, preserving
    // order of columns in right dataframe.
    leftTableTransformDF = leftTableTransformDF.selectExpr(rightTable.getDataFrame().columns());

    // Update the view of the transformed left dataframe
    leftTableTransformDF.createOrReplaceTempView(leftTable.getTempViewName());

    // Create a new AppleTable with transformed dataframe
    AppleTable leftTableTransform = new AppleTable(leftTable.getSourceType(), leftTableTransformDF,
        leftTable.getDelimiter(), leftTable.getTempViewName());




    // Comparison of transformed left dataframe and right dataframe
    Pair<Dataset<Row>, Dataset<Row>> result = SparkCompare
        .compareAppleTables(leftTableTransform, rightTable);

    Assert.assertEquals(0, result.getLeft().count());
    Assert.assertEquals(0, result.getRight().count());
  }
}
