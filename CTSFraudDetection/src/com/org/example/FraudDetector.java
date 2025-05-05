package com.org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class FraudDetector {

    private static final Logger logger = LoggerFactory.getLogger(FraudDetector.class);

    // UDFs
    private static void registerUDFs(SparkSession spark) {
        spark.udf().register("isHighAmount", (UDF1<Double, Boolean>) amount -> amount != null && amount > 10000, DataTypes.BooleanType);
        spark.udf().register("getReason", (UDF1<Double, String>) amount -> amount != null && amount > 10000 ? "High Value Transaction" : "", DataTypes.StringType);
    }

    public static Dataset<Row> detectFraud(SparkSession spark, Dataset<Row> customers, Dataset<Row> transactions) {
        logger.info("🔍 Running fraud detection...");

        registerUDFs(spark);

        Dataset<Row> transactionsProcessed = addUnixTimestamp(transactions);
        transactionsProcessed = addTransactionCount(transactionsProcessed);
        Dataset<Row> enriched = joinWithCustomers(transactionsProcessed, customers);
        Dataset<Row> flagged = applyFraudRules(enriched);

        logSuspiciousTransactions(flagged);

        logger.info("✅ Fraud detection completed.");
        return flagged;
    }

    private static Dataset<Row> addUnixTimestamp(Dataset<Row> transactions) {
        return transactions.withColumn("timestamp_unix", unix_timestamp(col("timestamp")));
    }

    private static Dataset<Row> addTransactionCount(Dataset<Row> transactions) {
        WindowSpec windowSpec = Window.partitionBy("account_id").orderBy(col("timestamp_unix")).rangeBetween(-300, 0);
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
                when(col("txn_count_in_5min").gt(5)
                        .or(callUDF("isHighAmount", col("amount"))), true).otherwise(false).alias("is_suspicious"),
                when(col("txn_count_in_5min").gt(5), lit("Rapid Transactions"))
                        .otherwise(when(callUDF("isHighAmount", col("amount")), lit("High Value Transaction"))
                                .otherwise(lit("Normal Transaction"))).alias("fraud_reason")
        );
    }

    private static void logSuspiciousTransactions(Dataset<Row> flagged) {
        flagged.filter("is_suspicious = true").foreach(row -> {
            try {
                String accountId = row.getAs("account_id");
                Double amount = row.getAs("amount");
                String reason = row.getAs("fraud_reason");
                String transactionId = row.getAs("transaction_id");

                logger.warn("⚠️ ALERT: Suspicious transaction detected! Account: {}, Amount: ${}, Reason: {}",
                        accountId, amount, reason);

                insertLog("WARN", "Suspicious transaction: $" + amount + ", Reason: " + reason,
                        accountId, transactionId, reason);
            } catch (Exception e) {
                logger.error("❌ Failed to log suspicious transaction: {}", e.getMessage());
            }
        });
    }

    public static void saveToDatabase(Dataset<Row> flagged) {
        logger.info("📁 Saving flagged fraud transactions to PostgreSQL...");

        Dataset<Row> fraudTransactions = flagged.filter(col("is_suspicious").equalTo(true));
        if (fraudTransactions.isEmpty()) {
            logger.info("✅ No fraudulent transactions to save.");
            return;
        }

        Properties props = getDatabaseProperties();
        if (props == null) return;

        String pgUrl = "jdbc:postgresql://ep-fancy-term-a1ddi401-pooler.ap-southeast-1.aws.neon.tech/fraud_detection_db?sslmode=require";

        try {
            fraudTransactions.write()
                    .option("header", "true")
                    .mode("append")
                    .jdbc(pgUrl, "fraud_transactions", props);

            logger.info("✅ Fraud transactions stored in PostgreSQL.");
        } catch (Exception e) {
            logger.error("❌ Error writing to DB: {}", e.getMessage());
        }
    }

    private static Properties getDatabaseProperties() {
        String user = System.getenv("DB_USER");
        String pass = System.getenv("DB_PASS");

        if (user == null || pass == null || user.isEmpty() || pass.isEmpty()) {
            logger.error("❌ DB credentials are missing. Set DB_USER and DB_PASS env vars.");
            return null;
        }

        Properties props = new Properties();
        props.put("user", user);
        props.put("password", pass);
        props.put("driver", "org.postgresql.Driver");
        return props;
    }

    public static void insertLog(String level, String message, String accountId, String transactionId, String reason) {
        Properties props = getDatabaseProperties();
        if (props == null) return;

        String jdbcUrl = "jdbc:postgresql://ep-fancy-term-a1ddi401-pooler.ap-southeast-1.aws.neon.tech/fraud_detection_db?sslmode=require";
        String insertSQL = "INSERT INTO fraud_logs (log_level, log_message, account_id, transaction_id, fraud_reason) VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
             PreparedStatement stmt = conn.prepareStatement(insertSQL)) {

            stmt.setString(1, level);
            stmt.setString(2, message);
            stmt.setString(3, accountId);
            stmt.setString(4, transactionId);
            stmt.setString(5, reason);
            stmt.executeUpdate();

        } catch (SQLException e) {
            logger.error("❌ Failed to insert log: {}", e.getMessage());
        }
    }
}
