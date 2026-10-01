package com.apache.spark;

import org.apache.spark.sql.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class ApacheSparkApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApacheSparkApplication.class, args);

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("Converting dataset into dataframe and viceversa")
                .master("local")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        String [] stringList= new String[] {"banana", "apple", "apple","mango","banana"};
        List<String> data = Arrays.asList(stringList);

        Dataset<String>ds= spark.createDataset(data, Encoders.STRING());
        System.out.println("count is here: "+ds.count());
        ds.printSchema();
        ds.show();
        Dataset<Row> data2= ds.groupBy("value").count();
        data2.show();
        spark.stop();


    }

}
