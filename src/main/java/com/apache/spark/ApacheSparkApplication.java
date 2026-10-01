package com.apache.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class ApacheSparkApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApacheSparkApplication.class, args);

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("Converting dataset into dataframe and vice versa")
                .master("local")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        String [] stringList= new String[] {"banana", "apple", "apple","mango","banana"};
        List<String> data = Arrays.asList(stringList);

        Dataset<String>ds= spark.createDataset(data, Encoders.STRING());

        //Implement Map function
        ds = ds.map((MapFunction<String, String>) row -> "word: "+ row, Encoders.STRING());
        ds.show(10);

        //Implment reduce function
        String stringValue = ds.reduce(new StringReducer());
        System.out.println("result of reduce method is: "+stringValue);

    }

    static class StringReducer implements ReduceFunction<String>, Serializable {

        @Override
        public String call(String v1, String v2) throws Exception {
            return v1+" "+v2;
        }
    }

}
