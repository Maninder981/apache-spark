package com.apache.spark;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheSparkApplication {

    public static void main(String[] args) {

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("Combine 2 different type of datasets")
                .master("local")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        SpringApplication.run(ApacheSparkApplication.class, args);
    }

}
