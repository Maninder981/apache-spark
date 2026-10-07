package com.apache.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.internal.config.R;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

import static org.apache.spark.sql.functions.desc;

@SpringBootApplication
public class ApacheSparkApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApacheSparkApplication.class, args);

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("Combine 2 different type of datasets")
                .master("local")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        String redditFile = "src/main/resources/Reddit_2007-small.json";

        //the below entire code finds the most frequently used words in Reddit comments, excluding stop words.
        Dataset<Row> redditDf = spark.read()
                .format("json")
                .option("inferSchema", "true")
                .option("header", true)
                .load(redditFile);

        //we are reading the body column from the json file because that content is main.
        redditDf = redditDf.select("body");

        Dataset<String> wordsDs = redditDf.flatMap((FlatMapFunction<Row, String>)
                r -> Arrays.asList(r.toString().replace("\n", "").replace("\r", "").trim().toLowerCase()
                        .split(" "))
                        .iterator(), Encoders.STRING());

        Dataset<Row> wordDf= wordsDs.toDF();

        Dataset<Row> boringWordsDf= spark.createDataset(Arrays.asList(WordUtils.stopWords),Encoders.STRING()).toDF();

        wordDf= wordDf.join(boringWordsDf,wordDf.col("value").equalTo(boringWordsDf.col("value")),"leftanti");

        wordDf=wordDf.groupBy("value").count();
        wordDf.orderBy(desc("count")).show();


        spark.stop();
    }

}
