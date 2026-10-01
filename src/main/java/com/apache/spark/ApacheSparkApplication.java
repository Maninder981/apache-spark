package com.apache.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheSparkApplication {

    public static void start(SparkSession sparkSession){

        String fileName= "src/main/resources/houses.csv";
        Dataset<Row> df= sparkSession.read()
                .format("csv")
                .option("inferSchema", "true") //make sure to use string version of true
                .option("header", true)
                .option("sep",";")
                .load(fileName);

        df.show();
        df.printSchema();

        System.out.println("House ingested in dataframe: "+df);
        Dataset<House> houseDataset = df.map(new HouseMapper(), Encoders.bean(House.class));
        System.out.println("house ingested in dataset");
        houseDataset.show();
        houseDataset.printSchema();

        Dataset<Row> df2= houseDataset.toDF();
        df2.show();

    }

    public static void main(String[] args) {

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("CSV to dataframe to DataSet<House> and back")
                .master("local")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        start(spark);

        SpringApplication.run(ApacheSparkApplication.class, args);
    }

}
