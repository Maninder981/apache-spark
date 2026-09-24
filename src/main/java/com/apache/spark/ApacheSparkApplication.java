package com.apache.spark;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static org.apache.spark.sql.functions.*;

@SpringBootApplication
public class ApacheSparkApplication {

    public static Dataset<Row> buildDurhamParksDataFrame(SparkSession sparkSession) {
        Dataset<Row> df = sparkSession.read()
                .format("json")
                .option("multiline", true)
                .load("src/main/resources/durham-parks.json");

        /**
         * In this example: we will combine 2 different datasets(One is csv and another is Json using spark)
         * In the following method: We are fetching the required data from the json file. Another method will fetch from csv.
         * the following df we are fetching the important information from the json file.
         * Adding more columns and putting values inside those columns from fetching from json file.
         * at the end using drop function, we are removing those columns from where we fetched the information because
         * we already fetched the required values so now the main fields are not required as it consume memory so dropping them.
         */
        df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"),
                        df.col("fields.objectid"), lit("_Durham")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", lit("Durham"))
                .withColumn("address", df.col("fields.address"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acres", df.col("fields.acres"))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0).cast("string"))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1).cast("string"))
                .drop("fields").drop("geometry").drop("record_timestamp").drop("recordid")
                .drop("datasetid");
        return df;

    }

    /**
     * Reading csv file and renaming columns names
     */
    private static Dataset<Row> buildPhilParksDataFrame(SparkSession sparkSession) {
        Dataset<Row> df = sparkSession.read()
                .format("csv")
                .option("multiline", true)
                .option("header", true)
                .load("src/main/resources/philadelphia_recreations.csv");

        //2 ways to filter the data. One is using spark and another one passing the SQL(which we currently using)
        //df= df.filter(lower(df.col("USE_")).like("%park%"));
        df = df.filter("lower(USE_) like '%park%' ");

        df = df.withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID")))
                .withColumnRenamed("ASSET_NAME", "park_name")
                .withColumn("city", lit("Philadelphia"))
                .withColumnRenamed("ADDRESS", "address")
                .withColumn("has_playground", lit("UNKNOWN"))
                .withColumnRenamed("ZIPCODE", "zipcode")
                .withColumnRenamed("ACREAGE", "land_in_acres")
                .withColumn("geoX", lit("UNKNOWN"))
                .withColumn("geoY", lit("UNKNOWN"))
                .drop("SITE_NAME")
                .drop("OBJECTID")
                .drop("CHILD_OF")
                .drop("TYPE")
                .drop("USE_")
                .drop("DESCRIPTION")
                .drop("SQ_FEET")
                .drop("ALLIAS")
                .drop("CHRONOLOGY")
                .drop("NOTES")
                .drop("DATE_EDITED")
                .drop("EDITED_BY")
                .drop("OCCUPANT")
                .drop("TENANT")
                .drop("LABEL");

        return df;
    }

    private static Dataset<Row> combineDataFrames(Dataset<Row> df1, Dataset<Row> df2) {
        //Match by column names using unionByName() method
        // If we use the union method, then it matches the column based on order, but in our case ordering is different
        // so using unionByName
        Dataset<Row> df = df1.unionByName(df2);
        System.out.println("We have :" + df.count() + " records");

        Partition[] partitions = df.rdd().partitions();
        System.out.println("total number of partitions: " + partitions.length);


        return df;
    }


    public static void main(String[] args) {
        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("Combine 2 different type of datasets")
                .master("local")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        SpringApplication.run(ApacheSparkApplication.class, args);

        Dataset<Row> durhamDf = buildDurhamParksDataFrame(spark);
        durhamDf.show(3);
        durhamDf.printSchema();
        Dataset<Row> secondDf = buildPhilParksDataFrame(spark);
        secondDf.printSchema();
        Dataset<Row> combine = combineDataFrames(durhamDf, secondDf);
//        secondDf.show(3);
        combine.show(3);

        spark.stop();
    }

}
