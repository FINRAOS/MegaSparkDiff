<h1>MegaSparkDiff</h1>

    MegaSparkDiff is an open source tool that helps you compare any pair
    combinations of the following
    
    MegaSparkDiff is an open source tool that helps you compare any pair
    combination of data sets that are of the following types: 

    (HDFS, JDBC, S3, Hbase, Text Files, Hive). 
    
    MegaSparkDiff can run on Amazon EMR (Elastic Map Reduce),
    Amazon EC2 instances and cloud environments
    with compatible Spark distributions.

How to Use form Within a Java or SCALA Project
----------------------------------------------
```sh
<dependency>
    <groupId>org.finra.megasparkdiff</groupId>
    <artifactId>mega-spark-diff</artifactId>
    <version>0.1</version>
</dependency>
```

SparkFactory
-----------
    It parallelizes source/target data.

    The data sources can be in following forms:
        Text File
        HDFS File
        SQL query over a JDBC data source
        Hive Table

SparkCompare
------------
    Compares pair combinations of supported sources,
    Please note in case of comparing a schema-based source to a non-schema based source, the SparkCompare
    class will attempt to flatten the schema based source to delimited values and then do the comparison. The delimiter
    can be specified while launching the compare job.

How to use via shell script in EMR
----------------------------------
    There will exist a shell script named a3a.sh that will wrap around
    this Java/Scala project.  This script will accept several parameters
    related to source definitions, output destination, and run
    configurations, as well as which two data sets to compare.
    
    The parameters are as follows:
        -ds=<data_source_folder>: The folder where the database
            connection parameters and data queries reside
        -od=<output_directory>: The directory where MegaSparkDiff will write
            its output
        -rc=<run_config_file_name>: The file that will be used to load
            any special run and Spark configurations.  This parameter is
            optional
            
    To specify a data set to compare, pass in the name of one of the
    data queries found in a config file inside <data_source_folder>
    prepended by "--".  The program will execute the queries assigned to
    the names passed into the command line, store them into tables, and
    perform the comparison.
    
    Example call:
        ./msd.sh -ds=./data_sources/ -od=output --shraddha --carlos
        
    Additionally, the user will have the option to add JDBC Driver jar
    files by including them in the classpath.  This is to enable them to
    extract from whichever database they choose.
