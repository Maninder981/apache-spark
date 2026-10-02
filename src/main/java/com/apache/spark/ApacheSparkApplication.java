package com.apache.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

        String fileName="src/main/resources/students.csv";

        // read student information from file
        Dataset<Row> studentDf= spark.read()
                .format("csv")
                .option("inferSchema","true")
                .option("header",true)
                .load(fileName);

        //read grade chart file
        String gradeChartFile= "src/main/resources/grade_chart.csv";
        Dataset<Row> gradesDf= spark.read()
                .format("csv")
                .option("inferSchema","true")
                .option("header", true)
                .load(gradeChartFile);

        /**
         * Joining student data with grade data where GPA is same for both dataset and then apply filter on the top of it.
         * where gpa greater than 3.0 and less than 4.5
         * or grades equal to 1.0 so that it will show the students list who are topper and failed.
         */
        Dataset<Row> filterDf= studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("GPA")))
                .filter(gradesDf.col("gpa").gt(3.0).and(gradesDf.col("gpa").lt(4.5))
                        .or(gradesDf.col("gpa").equalTo(1.0)))
                .select("student_name","favorite_book_title","letter_grade");

        filterDf.show();


    }

}
