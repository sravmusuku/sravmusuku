package com.org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class FraudCheck {

    private static final Logger logger = LoggerFactory.getLogger(FraudCheck.class);

    public static void main(String[] args) {
        logger.info("🚀 Starting full system check...");

        // ✅ Step 1: Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("FraudCheckSystem")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        // ✅ Step 2: Load customer and transaction data
        Dataset<Row> customers = loadCustomers(spark);
        Dataset<Row> transactions = loadTransactions(spark);

        // ✅ Step 3: Apply fraud detection
        Dataset<Row> flaggedTransactions = detectFraud(spark, customers, transactions);

        // ✅ Step 4: Log fraud alerts
        flaggedTransactions.filter("is_suspicious = true").foreach(row -> {
            logger.warn("⚠️ ALERT: Fraud detected! Account: {}, Amount: ${}, Reason: {}",
                    row.getAs("account_id"), row.getAs("amount"), row.getAs("reason"));
        });

        // Step 5: Save flagged fraud transactions to PostgreSQL
        saveToDatabase(flaggedTransactions);

        // Step 6: Stop Spark session
        spark.stop();
        logger.info("✅ System check completed successfully!");
    }

    private static Dataset<Row> loadCustomers(SparkSession spark) {
        logger.info("📥 Loading customer data...");
        return spark.read()
                .option("multiline", "true")
                .json("customer.json")
                .withColumnRenamed("status", "customer_status");
    }

    private static Dataset<Row> loadTransactions(SparkSession spark) {
        logger.info("📥 Loading transaction data...");
        return spark.read()
                .option("multiline", "true")
                .json("transactions.json")
                .withColumn("amount", col("amount").cast("double"));
    }

    private static Dataset<Row> detectFraud(SparkSession spark, Dataset<Row> customers, Dataset<Row> transactions) {
        logger.info("🔍 Running fraud detection...");

        //  Register UDFs
        spark.udf().register("isHighAmount", (UDF1<Double, Boolean>) amount -> amount != null && amount > 10000,
                DataTypes.BooleanType);

        spark.udf().register("getReason", (UDF1<Double, String>) amount -> amount != null && amount > 10000 ? "High Value Transaction" : "",
                DataTypes.StringType);

        // Define window specification
        WindowSpec windowSpec = Window.partitionBy("account_id").orderBy("timestamp");

        // Process transactions
        Dataset<Row> transactionsProcessed = transactions
                .withColumn("prev_transaction_time", lag("timestamp", 1).over(windowSpec))
                .withColumn("time_diff", unix_timestamp(coalesce(col("timestamp"), lit("1970-01-01 00:00:00")))
                        .minus(unix_timestamp(coalesce(col("prev_transaction_time"), lit("1970-01-01 00:00:00")))))
                .withColumn("transaction_count_in_window", count("transaction_id").over(windowSpec.rowsBetween(-1, 0)))
                .withColumn("isRapidTransaction", when(col("transaction_count_in_window").gt(1), true).otherwise(false));

        // Join customers
        Dataset<Row> enrichedTransactions = transactionsProcessed.join(customers, "account_id");

        //  Apply fraud detection rules
        return enrichedTransactions
                .select(
                        col("account_id"),
                        col("name").alias("customer_name"),
                        col("transaction_id"),
                        col("transaction_type"),
                        col("amount"),
                        col("location"),
                        col("merchant_id"),
                        col("account_type"),
                        col("customer_status"),
                        when(col("isRapidTransaction")
                                .or(callUDF("isHighAmount", col("amount"))), true)
                                .otherwise(false).alias("is_suspicious"),
                        when(col("isRapidTransaction"), "Rapid Transaction")
                                .otherwise(when(callUDF("isHighAmount", col("amount")), "High Value Transaction")
                                        .otherwise("Normal Transaction")).alias("reason")
                );
    }

    private static void saveToDatabase(Dataset<Row> flagged) {
        logger.info("Saving flagged transactions to PostgreSQL...");

        String pgConnectionUrl = "jdbc:postgresql://your-db-host/fraud_detection_db?sslmode=require";
        Properties connectionProps = new Properties();
        connectionProps.put("user", System.getenv("DB_USER"));
        connectionProps.put("password", System.getenv("DB_PASS"));
        connectionProps.put("driver", "org.postgresql.Driver");

        flagged.write()
                .option("header", "true")
                .mode("append")
                .jdbc(pgConnectionUrl, "fraud_trans", connectionProps);

        logger.info("✅ Fraud transactions successfully stored in PostgreSQL.");
    }
}

