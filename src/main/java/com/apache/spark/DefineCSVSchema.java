package com.apache.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCSVSchema {


    public void printSchema() {
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV to Dataframe")
                .master("local")
                .getOrCreate();

        /**
         * This is the way to define custom schema in apache spark.
         *
         */
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "id",
                        DataTypes.IntegerType,
                        false), // it means this column cannot be null
                DataTypes.createStructField(
                        "product_id",
                        DataTypes.IntegerType,
                        true),
                DataTypes.createStructField(
                        "item_name",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "published_on",
                        DataTypes.DateType,
                        true),
                DataTypes.createStructField(
                        "url",
                        DataTypes.StringType,
                        false)
        });

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
                .option("sep", ";")
                .option("quote", "^")
                .option("dateFormat", "M/d/y")
                .schema(schema)
                .load("src/main/resources/amazonProducts.txt");

//        df.show();
        //It will tell you how many lines you want to show and how many characters you want to define.
        df.show(5,15);
        df.printSchema();
    }
}
