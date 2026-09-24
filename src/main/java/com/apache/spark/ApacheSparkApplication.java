package com.apache.spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

@SpringBootApplication
public class ApacheSparkApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApacheSparkApplication.class, args);

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        // get data
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("src/main/resources/name_and_comments.txt");


       // in case we need to concat 2 columns from existing columns
        df = df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")));

        //in case we want to see the only columns which have numbers in it.
        df= df.filter(df.col("comment").rlike("\\d+"));

        df.show(3);

//		// Write to destination
//		String dbConnectionUrl = "jdbc:postgresql://localhost/course_data"; // <<- You need to create this database
//		Properties prop = new Properties();
//		prop.setProperty("driver", "org.postgresql.Driver");
//		prop.setProperty("user", "postgres");
//		prop.setProperty("password", "password"); // <- The password you used while installing Postgres
//
//		df.write()
//				.mode(SaveMode.Overwrite)
//				.jdbc(dbConnectionUrl, "project1", prop);


    }

}
