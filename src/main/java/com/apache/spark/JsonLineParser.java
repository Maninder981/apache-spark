package com.apache.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonLineParser {

    public void printSchema() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();

        Dataset<Row> df= spark.read().format("json").load("src/main/resources/simple.json");
//        df.show();

        Dataset<Row> df2= spark.read().format("json").option("multiline", true).load("src/main/resources/multiline.json");
        df2.printSchema();
        df2.show(5,150);
        spark.stop();
    }
}
