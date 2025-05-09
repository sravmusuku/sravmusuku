package org.example;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkDemo")
                .master("local[*]")
                .getOrCreate();

        try {
            // Read the CSV file into a DataFrame
            Dataset<Row> customers = spark.read()
                    .option("header", "true")
                    .csv("customers.csv");

            customers.show();
            System.out.println("Total no of actual records: " + customers.count());

            // Filter null values
            Dataset<Row> filteredCustomers = customers.filter(
                    col("email").isNotNull().and(col("balance").isNotNull())
            );

            filteredCustomers.show();
            System.out.println("Total no of filtered records: " + filteredCustomers.count());

            // Display current date
            Dataset<Row> currentDateDS = spark.sql("SELECT current_date() AS Today");
            currentDateDS.show();

            // Add a new column with the current date
            Dataset<Row> newCustomers = filteredCustomers.withColumn("account_created", current_date());
            newCustomers.show();

            // Write the filtered DataFrame to a CSV file
            newCustomers.write()
                    .mode("append")
                    .option("header", "true")
                    .option("delimiter", ",")
                    .csv("output/currentcustomers.csv");

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            spark.stop();
        }
    }
}