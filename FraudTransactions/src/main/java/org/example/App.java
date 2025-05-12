package org.example;

import java.util.Properties;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class App {
    public static void main(String[] args) {
        // Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("JavaSparkETLPipeline")
                .master("local[*]")
                .getOrCreate();

        try {
            // Extract Step: Load Transactions from JSON & Customers from CSV
            String transactionsFile = "transaction_data.json";
            String customersFile = "customer_data.csv";

            Dataset<Row> transactionds = spark.read()
                    .option("multiline", "true")
                    .option("inferSchema", "true")
                    .json(transactionsFile);

            Dataset<Row> customerds = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(customersFile);

            transactionds.createOrReplaceTempView("transactions");
            customerds.createOrReplaceTempView("customers");

            // Transform Step 1: Retrieve High-Amount Transactions (Amount > 1000)
            Dataset<Row> high_amount_transactions = spark.sql(
                    "SELECT TransactionId, SenderAccount, TransactionAmount, 'HighAmount' AS FraudType " +
                            "FROM transactions WHERE TransactionAmount > 1000"
            );

            // Transform Step 2: Retrieve Rapid Transactions (More than 5 transactions in 5 minutes)
            Dataset<Row> rapid_transactions = spark.sql(
                    "SELECT TransactionId, SenderAccount, TransactionAmount, 'RapidTransactions' AS FraudType " +
                            "FROM transactions WHERE SenderAccount IN ( " +
                            "   SELECT SenderAccount FROM transactions " +
                            "   WHERE Timestamp BETWEEN (CURRENT_TIMESTAMP - INTERVAL 5 MINUTE) AND CURRENT_TIMESTAMP " +
                            "   GROUP BY SenderAccount HAVING COUNT(*) > 5 " +
                            ")"
            );

            // Transform Step 3: Combine Both Frauds Into a Single Dataset
            Dataset<Row> combined_transactions = high_amount_transactions.union(rapid_transactions);
            combined_transactions.show();

            // Load Step: Save Flagged Transactions into PostgreSQL
            Properties connectionProps = new Properties();
            connectionProps.put("user", "your_db_user");
            connectionProps.put("password", "your_db_password");
            connectionProps.put("driver", "org.postgresql.Driver");

            String pgconnectionUrl = "jdbc:postgresql://your-db-url/banking";

            combined_transactions.write()
                    .mode(SaveMode.Append)
                    .jdbc(pgconnectionUrl, "fraud_transactions", connectionProps);

            System.out.println("Fraudulent transactions stored successfully!");

        } catch (Exception e) {
            System.err.println("Error during ETL pipeline execution: " + e.getMessage());
        } finally {
            spark.stop();
        }
    }
}

