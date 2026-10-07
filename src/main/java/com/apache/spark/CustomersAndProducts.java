package com.apache.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CustomersAndProducts {

    public void start(SparkSession spark) {

        String customerFileName = "src/main/resources/customers.csv";

        // Read customer information from file
        Dataset<Row> customerDf = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", true)
                .load(customerFileName);

        // Read product information from file
        String productFileName = "src/main/resources/products.csv";

        Dataset<Row> productDf = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", true)
                .load(productFileName);


        // Read purchase information from file
        String purchaseFileName = "src/main/resources/purchases.csv";
        Dataset<Row> purchaseDf = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", true)
                .load(purchaseFileName);

        /**
         *
         *  Join all datasets together. If you see it will give u duplicate entries as same customer shop multiple items.
         *  so we need to show in the way that how many items one customer buy and how much he spent.
         *  if you see in the step2 we are applying count, max and other functions to find it.
         */
        //
        Dataset<Row> combineAll = customerDf.join(purchaseDf, customerDf.col("customer_id")
                        .equalTo(purchaseDf.col("customer_id")))
                .join(productDf, purchaseDf.col("product_id").equalTo(productDf.col("product_id")))
                .drop("favorite_website").drop(purchaseDf.col("customer_id"))
                .drop(purchaseDf.col("product_id")).drop("product_id");

        combineAll.groupBy("first_name").agg(
                        count("product_name").as("number_of_purchases")
                        , max("product_price").as("most_exp_purchase")
                        , sum("product_price").as("total_spent"))
                .show();

    }
}
