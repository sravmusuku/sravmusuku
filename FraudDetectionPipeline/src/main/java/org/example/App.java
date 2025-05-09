package org.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;

import org.apache.spark.sql.SaveMode;
import java.util.Properties;

public class App {

    public static void main(String[] args) {

        // ✅ Step 1: Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("FraudDetectionApp")
                .master("local[*]")  // Uses all available cores
                .config("spark.sql.shuffle.partitions", "4")  // Optimizes shuffle performance
                .getOrCreate();

        // ✅ Step 2: Load customer and transaction data using the Account & Customer classes
        Dataset<Row> customers = Customers.readCustomers(spark, "customer.json");
        Dataset<Row> transactions = Transactions.readTransactions(spark, "transactions.json");
        //transactions.show();

        // ✅ Step 3: Call the FraudDetector's detectFraud method
        Dataset<Row> flaggedTransactions = FraudDetector.detectFraud(spark, customers, transactions);

        // ✅ Step 4: Show results (limiting to avoid excessive output)
        flaggedTransactions.show(false);

        String pgconnectionUrl = "jdbc:postgresql://ep-fancy-term-a1ddi401-pooler.ap-southeast-1.aws.neon.tech/fraud_detection_db?sslmode=require";
        //postgresql://Banking_owner:npg_NPTmM9bdCoZ5@ep-fancy-term-a1ddi401-pooler.ap-southeast-1.aws.neon.tech/fraud_detection_db?sslmode=require
        Properties connectionProps = new Properties();
        connectionProps.put("user","Banking_owner");
        connectionProps.put("password","npg_NPTmM9bdCoZ5");
        connectionProps.put("driver","org.postgresql.Driver");

        String output_tablename = "Enriched_Transactions";

        flaggedTransactions.write()
                .option("header","true")
                .mode(SaveMode.Overwrite)
                .jdbc(pgconnectionUrl, output_tablename, connectionProps);

        // ✅ Step 5: Stop Spark session
        spark.stop();
    }
}

