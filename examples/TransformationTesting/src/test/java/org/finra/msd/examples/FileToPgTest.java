package org.finra.msd.examples;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.finra.msd.containers.AppleTable;
import org.finra.msd.examples.db.PostgresDatabase;
import org.finra.msd.sparkcompare.SparkCompare;
import org.finra.msd.sparkfactory.SparkFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileToPgTest {

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
  }

  @After
  public void tearDown() throws IOException, SQLException {
    PostgresDatabase.tearDown();
  }

  @Test
  public void testTransformFileToPg() throws SQLException {
    SparkFactory.initializeSparkLocalMode("local[*]", "WARN", "1");

    // Parallelize the source text file
    AppleTable leftTable = SparkFactory
        .parallelizeTextSource(FileToPgTest.class.getResource(
            "/appliance_source.txt").getPath(),
            "appliance_left");

    // Parallelize the target data
    AppleTable rightTable = SparkFactory
        .parallelizeJDBCSource("org.postgresql.Driver",
            PostgresDatabase.getUrl(),
            PostgresDatabase.getProperties().getProperty("user"),
            PostgresDatabase.getProperties().getProperty("password"),
            "(select * from appliance) a", "appliance_right");

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
        (String x, String y) -> Integer.valueOf(
            new BigDecimal(x).divide(new BigDecimal(y), BigDecimal.ROUND_HALF_UP).setScale(0, BigDecimal.ROUND_HALF_UP).toString()
        ),
        DataTypes.IntegerType);

    // Handle the capitalization the first letter of each word of the source's "TYPE".
    SparkFactory.sparkSession().udf().register("capitalize_type",
        (String x) -> WordUtils.capitalize(x),
        DataTypes.StringType);




    // Create a list of the source column names
    List<String> fieldNamesLeft = Arrays
        .asList("name", "type", "sales_amount", "price", "date_added");

    // Create the schema for each column with the String data type and nullable property of true
    List<StructField> structFieldsLeft = new ArrayList<>();
    for (String fieldNameLeft : fieldNamesLeft) {
      structFieldsLeft.add(DataTypes.createStructField(fieldNameLeft, DataTypes.StringType, true));
    }

    StructType leftSchema = DataTypes.createStructType(structFieldsLeft);

    // Create a dataframe containing the schema defined above, with data populated by splitting the
    // text file by character ";"
    Dataset<Row> leftTableTransformDF = leftTable.getDataFrame().map((MapFunction<Row, Row>) x -> {
      Object[] columns = x.getString(0).split(";");
      return RowFactory.create(columns);
    }, RowEncoder.apply(leftSchema));




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

    // Flatten the transformed dataframe
    leftTableTransformDF = SparkFactory.flattenDataFrame(leftTableTransformDF, ",");

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
