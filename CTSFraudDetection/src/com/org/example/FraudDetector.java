package com.org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class FraudDetector {

    private static final Logger logger = LoggerFactory.getLogger(FraudDetector.class);

    public static Dataset<Row> detectFraud(SparkSession spark, Dataset<Row> customers, Dataset<Row> transactions) {
        logger.info("🔍 Running fraud detection with 5-minute transaction rule...");

        // Register UDFs
        spark.udf().register("isHighAmount", (UDF1<Double, Boolean>) amount -> amount != null && amount > 10000, DataTypes.BooleanType);
        spark.udf().register("getReason", (UDF1<Double, String>) amount -> amount != null && amount > 10000 ? "High Value Transaction" : "", DataTypes.StringType);

        // ✅ Step 1: Add timestamp as unix format
        Dataset<Row> transactionsProcessed = transactions
                .withColumn("timestamp_unix", unix_timestamp(col("timestamp")));

        // ✅ Step 2: Apply range-based window (past 5 minutes for the same account)
        WindowSpec fiveMinWindow = Window
                .partitionBy("account_id")
                .orderBy(col("timestamp_unix"))
                .rangeBetween(-300, 0); // 5-minute sliding window

        // ✅ Step 3: Count transactions in that window
        transactionsProcessed = transactionsProcessed
                .withColumn("txn_count_in_5min", count("transaction_id").over(fiveMinWindow));

        // ✅ Step 4: Join with customers
        Dataset<Row> enrichedTransactions = transactionsProcessed.join(customers, "account_id");

        // ✅ Step 5: Apply fraud rules
        Dataset<Row> flagged = enrichedTransactions
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
                        col("timestamp"),
                        col("txn_count_in_5min"),
                        when(col("txn_count_in_5min").gt(5)
                                .or(callUDF("isHighAmount", col("amount"))), true)
                                .otherwise(false).alias("is_suspicious"),
                        when(col("txn_count_in_5min").gt(5), lit("Rapid Transactions"))
                                .otherwise(when(callUDF("isHighAmount", col("amount")), lit("High Value Transaction"))
                                        .otherwise(lit("Normal Transaction"))).alias("fraud_reason")
                );

        // ✅ Log only suspicious transactions
        flagged.filter("is_suspicious = true").foreach(row -> {
            logger.warn("⚠️ ALERT: Suspicious transaction detected! Account: {}, Amount: ${}, Reason: {}",
                    row.getAs("account_id"), row.getAs("amount"), row.getAs("fraud_reason"));
        });

        logger.info("✅ Fraud detection completed.");
        return flagged;
    }

    public static void saveToDatabase(Dataset<Row> flagged) {
        logger.info("📁 Saving flagged fraud transactions to PostgreSQL...");

        // ✅ Secure PostgreSQL connection credentials
        String dbUser = System.getenv("DB_USER");
        String dbPass = System.getenv("DB_PASS");

        // ✅ Ensure credentials are available & not empty
        if (dbUser == null || dbPass == null || dbUser.isEmpty() || dbPass.isEmpty()) {
            logger.error("❌ ERROR: Database credentials are missing or empty! Please set DB_USER and DB_PASS.");
            return;
        }

        // ✅ Correct filtering: Boolean condition instead of string comparison
        Dataset<Row> fraudTransactions = flagged.filter(col("is_suspicious").equalTo(true));

        if (fraudTransactions.isEmpty()) {
            logger.info("✅ No fraudulent transactions detected. Skipping database save.");
            return;
        }

        // ✅ PostgreSQL Connection Setup
        String pgConnectionUrl = "jdbc:postgresql://ep-fancy-term-a1ddi401-pooler.ap-southeast-1.aws.neon.tech/fraud_detection_db?sslmode=require";
        Properties connectionProps = new Properties();
        connectionProps.put("user", dbUser);
        connectionProps.put("password", dbPass);
        connectionProps.put("driver", "org.postgresql.Driver");

        try {
            fraudTransactions.write()
                    .option("header", "true")
                    .mode("append")
                    .jdbc(pgConnectionUrl, "fraud_transactions", connectionProps);

            logger.info("✅ Fraud transactions successfully stored in PostgreSQL.");
        } catch (Exception e) {
            logger.error("❌ Database insertion failed: ", e);
        }
    }
}
