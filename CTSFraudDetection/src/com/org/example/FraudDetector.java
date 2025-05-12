package com.org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class FraudDetector {

    private static final Logger logger = LoggerFactory.getLogger(FraudDetector.class);    

    // Thresholds for fraud detection
    private static final double HIGH_AMOUNT_THRESHOLD = 100000;
    private static final int TRANSACTION_COUNT_THRESHOLD = 5;

    // Register UDFs for fraud detection
    private static void registerUDFs(SparkSession spark) {
        spark.udf().register("isHighAmount", (UDF1<Double, Boolean>) amount -> 
                amount != null && amount > HIGH_AMOUNT_THRESHOLD, DataTypes.BooleanType);
    }

    public static Dataset<Row> detectFraud(SparkSession spark, Dataset<Row> customers, Dataset<Row> transactions) {
        logger.info("Running fraud detection...");

        registerUDFs(spark);

        // 🔹 Remove null values before processing
        transactions = transactions.na().drop("any");

        Dataset<Row> transactionsProcessed = addUnixTimestamp(transactions);
        transactionsProcessed = addTransactionCount(transactionsProcessed);
        Dataset<Row> enriched = joinWithCustomers(transactionsProcessed, customers);
        Dataset<Row> flagged = applyFraudRules(enriched);

        logSuspiciousTransactions(flagged);
        DatabaseManager.saveFraudTransactions(flagged);

        logger.info("Fraud detection completed.");
        return flagged;
    }

    private static Dataset<Row> addUnixTimestamp(Dataset<Row> transactions) {
        return transactions.withColumn("timestamp_unix", unix_timestamp(col("timestamp")));
    }

    private static Dataset<Row> addTransactionCount(Dataset<Row> transactions) {
        WindowSpec windowSpec = Window.partitionBy("account_id")
                .orderBy(col("timestamp_unix"))
                .rangeBetween(-300, 0);
        
        return transactions.withColumn("txn_count_in_5min", count("transaction_id").over(windowSpec));
    }

    private static Dataset<Row> joinWithCustomers(Dataset<Row> transactions, Dataset<Row> customers) {
        return transactions.join(customers, "account_id");
    }

    private static Dataset<Row> applyFraudRules(Dataset<Row> dataset) {
        return dataset.select(
                col("account_id"),
                col("name").alias("customer_name"),
                col("transaction_id"),
                col("transaction_type"),
                col("amount"),
                col("location"),
                col("merchant_id"),
                col("account_type"),
                col("customer_status"),
                col("timestamp"),
                col("txn_count_in_5min"),
                when(col("txn_count_in_5min").gt(TRANSACTION_COUNT_THRESHOLD)
                        .or(callUDF("isHighAmount", col("amount"))), lit(true))
                        .otherwise(lit(false)).alias("is_suspicious"),
                when(col("txn_count_in_5min").gt(TRANSACTION_COUNT_THRESHOLD)
                        .and(callUDF("isHighAmount", col("amount"))), lit("Rapid & High Value Transaction"))
                .when(col("txn_count_in_5min").gt(TRANSACTION_COUNT_THRESHOLD), lit("Rapid Transaction"))
                .when(callUDF("isHighAmount", col("amount")), lit("High Value Transaction"))
                .otherwise(lit("Normal Transaction")).alias("fraud_reason")
        );
    }

    private static void logSuspiciousTransactions(Dataset<Row> flagged) {
        flagged.filter("is_suspicious = true").collectAsList().forEach(row -> {
            try {
                String accountId = row.getAs("account_id");
                Double amount = row.getAs("amount");
                String reason = row.getAs("fraud_reason");
                String transactionId = row.getAs("transaction_id");

                logger.warn(" ALERT: Suspicious transaction detected! Account: {}, Amount: ${}, Reason: {}",
                        accountId, amount, reason);

                DatabaseManager.insertFraudLog("WARN", "Suspicious transaction: $" + amount + ", Reason: " + reason,
                        accountId, transactionId, reason);
            } catch (Exception e) {
                logger.error(" Failed to log suspicious transaction: {}", e.getMessage());
            }
        });
    }
}

