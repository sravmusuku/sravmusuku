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
        logger.info("🔍 Running fraud detection...");

        // ✅ Register UDFs for fraud rules
        spark.udf().register("isHighAmount", (UDF1<Double, Boolean>) amount -> amount != null && amount > 10000,
                DataTypes.BooleanType);

        spark.udf().register("getReason", (UDF1<Double, String>) amount -> amount != null && amount > 10000 ? "High Value Transaction" : "",
                DataTypes.StringType);

        // ✅ Define window specification for rapid transaction detection
        WindowSpec windowSpec = Window.partitionBy("account_id").orderBy("timestamp");

        // ✅ Process transactions with fraud detection logic
        Dataset<Row> transactionsProcessed = transactions
                .withColumn("prev_transaction_time", lag("timestamp", 1).over(windowSpec))
                .withColumn("time_diff", unix_timestamp(coalesce(col("timestamp"), lit("1970-01-01 00:00:00")))
                        .minus(unix_timestamp(coalesce(col("prev_transaction_time"), lit("1970-01-01 00:00:00")))))
                .withColumn("transaction_count_in_window", count("transaction_id").over(windowSpec.rowsBetween(-1, 0)))
                .withColumn("isRapidTransaction", when(col("transaction_count_in_window").gt(1), true).otherwise(false));

        // ✅ Join customers
        Dataset<Row> enrichedTransactions = transactionsProcessed.join(customers, "account_id");

        // ✅ Apply fraud detection rules
        Dataset<Row> flagged = enrichedTransactions
                .select(
                        col("account_id"),
                        col("transaction_id"),
                        col("transaction_type"),
                        col("amount"),
                        col("location"),
                        col("merchant_id"),
                        when(col("isRapidTransaction")
                                .or(callUDF("isHighAmount", col("amount"))), true)
                                .otherwise(false).alias("is_suspicious"),
                        when(col("isRapidTransaction"), "Rapid Transaction")
                                .otherwise(when(callUDF("isHighAmount", col("amount")), "High Value Transaction")
                                        .otherwise("Normal Transaction")).alias("fraud_reason")
                );

        // ✅ Log fraud alerts
        flagged.filter(col("is_suspicious").equalTo(true)).foreach(row -> {
            logger.warn("⚠️ ALERT: Fraud detected! Account: {}, Amount: ${}, Reason: {}", 
                    row.getAs("account_id"), row.getAs("amount"), row.getAs("fraud_reason"));
        });

        logger.info("✅ Fraud detection completed successfully.");
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
