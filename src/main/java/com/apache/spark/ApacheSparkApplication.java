package com.apache.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheSparkApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApacheSparkApplication.class, args);
//        InferCSVSchema inferCSVSchema = new InferCSVSchema();
//        inferCSVSchema.printSchema();
//        DefineCSVSchema defineCSVSchema= new DefineCSVSchema();
//        defineCSVSchema.printSchema();
        JsonLineParser jsonLineParser= new JsonLineParser();
        jsonLineParser.printSchema();
    }

}
