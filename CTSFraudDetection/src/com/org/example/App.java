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
        logger.info("🚀 Starting full system check...");

        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("FraudCheckSystem")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        try {
            //  Load customer and transaction data
            Dataset<Row> customers = Customers.readCustomers(spark, "customer.json");
            Dataset<Row> transactions = Transactions.readTransactions(spark, "transactions.json");

            logger.info("Data successfully loaded!");

            //  Apply fraud detection
            Dataset<Row> flaggedTransactions = FraudDetector.detectFraud(spark, customers, transactions);

            // Save flagged fraud transactions to PostgreSQL
            saveToDatabase(flaggedTransactions);

        } catch (Exception e) {
            logger.error(" Error in fraud detection process: ", e);
        } finally {
            // Stop Spark session
            spark.stop();
            logger.info("System check completed successfully!");
        }
    }

    private static void saveToDatabase(Dataset<Row> flagged) {
        logger.info("Saving flagged transactions to PostgreSQL...");

        //  Secure PostgreSQL connection credentials
        String pgConnectionUrl = "jdbc:postgresql://ep-fancy-term-a1ddi401-pooler.ap-southeast-1.aws.neon.tech/fraud_detection_db?sslmode=require";
        Properties connectionProps = new Properties();
        connectionProps.put("user", System.getenv("DB_USER"));  // Using environment variables
        connectionProps.put("password", System.getenv("DB_PASS"));
        connectionProps.put("driver", "org.postgresql.Driver");

        try {
            flagged.write()
                    .option("header", "true")
                    .mode("append")
                    .jdbc(pgConnectionUrl, "fraud_transactions", connectionProps);

            logger.info(" Fraud transactions successfully stored in PostgreSQL.");

        } catch (Exception e) {
            logger.error("❌ Database insertion failed: ", e);
        }
    }
}
