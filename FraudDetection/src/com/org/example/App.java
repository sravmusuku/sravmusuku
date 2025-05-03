package com.org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {

    public static void main(String[] args) {

        // ✅ Step 1: Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("FraudDetectionApp")
                .master("local[*]")  // Uses all available cores
                .config("spark.sql.shuffle.partitions", "4")  // Optimizes shuffle performance
                .getOrCreate();

        // ✅ Step 2: Load customer and transaction data using the Account & Customer classes
        Dataset<Row> customers = Customer.readCustomers(spark, "customer.csv");
        Dataset<Row> transactions = Account.readTransactions(spark, "transaction.json");

        // ✅ Step 3: Call the FraudDetector's detectFraud method
        Dataset<Row> flaggedTransactions = FraudDetector.detectFraud(spark, customers, transactions);

        // ✅ Step 4: Show results (limiting to avoid excessive output)
        flaggedTransactions.limit(10).show(false);

        // ✅ Step 5: Stop Spark session
        spark.stop();
    }
}
