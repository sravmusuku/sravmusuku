package com.org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        logger.info(" Starting full system check...");

        SparkSession spark = SparkSession.builder()
                .appName("FraudCheckSystem")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        try {
            // Load input data
            Dataset<Row> customers = Customers.readCustomers(spark, "realistic_customers.json");
            Dataset<Row> transactions = Transactions.readTransactions(spark, "realistic_transactions.json");

            logger.info("Data loaded successfully!");

            // Run fraud detection
            Dataset<Row> flagged = FraudDetector.detectFraud(spark, customers, transactions);

       

        } catch (Exception e) {
            logger.error(" Error in fraud detection process: ", e);
        } finally {
            spark.stop();
            logger.info("System check completed successfully!");
        }
    }
}