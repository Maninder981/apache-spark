package com.apache.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

@Component
public class InferCSVSchema {

    public void printSchema(){
        SparkSession spark= SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();

        /**
         * multiline: true- It means data can be available in the multiline
         * sep: data is separated using ";" sign
         * quote: ^, it means where ever it will find the ^ sign, replace it with quotes.
         * dataFormat: should be in M/d/y format only.
         * inferSchema: true: It means we are requesting spark to tell us the schema based on the data. it will access the data and will tell us datatype.
         *
         */
        Dataset<Row> df = spark.read().format("CSV")
                .option("header", "true")
                .option("multiline", true)
                .option("sep",";")
                .option("quote","^")
                .option("dateFormat","M/d/y")
                .option("inferSchema", true)
                .load("src/main/resources/amazonProducts.txt");

        System.out.println("Dataframe content");
        df.show();
    }
}
