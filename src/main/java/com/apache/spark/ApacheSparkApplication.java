package com.apache.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheSparkApplication {

    /**
     * This code helps to find the most common words used by shakespeare in this life. We will
     * provide the unstructured text file and then see which words he used most.
     * @param sparkSession
     */
    public static void start(SparkSession sparkSession){
        String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" +
                "'for', 'if', 'in', 'into', 'is', 'it',\r\n" +
                "'no', 'not', 'of', 'on', 'or', 'such',\r\n" +
                "'that', 'the', 'their', 'then', 'there', 'these',\r\n" +
                "'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," +
                "'your', 'you', 'I', "
                + " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";

        String filename = "src/main/resources/shakespeare.txt";

        Dataset<Row> df= sparkSession.read().format("text").load(filename);

        Dataset<String> wordDs= df.flatMap(new LineMapper(), Encoders.STRING());
        Dataset<Row> df2= wordDs.toDF();
        /**
         * here if notice we are modifying 3 time of dataframe.
         * groupBy, OrderBy and filter.
         * good approach was to apply filter first and then apply orderBy and groupBy, because after apply
         * filter it removes some records and then would be good to apply order by on less records.
         * but spark is smart enough to handle these automatically.
         */
        df2= df2.groupBy("value").count();
        df2=df2.orderBy(df2.col("count").desc());
        df2=df2.filter("lower(value) NOT IN" +boringWords);

        df2.show();
    }

    public static void main(String[] args) {

        // Create a session
        SparkSession spark = new SparkSession.Builder()
                .appName("unstructured text to flap map")
                .master("local")
                .config("spark.ui.enabled", "false")
                .getOrCreate();



        SpringApplication.run(ApacheSparkApplication.class, args);
        start(spark);
        spark.stop();
    }

}
