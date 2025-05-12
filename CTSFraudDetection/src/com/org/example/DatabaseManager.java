package com.org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.spark.sql.functions.col;


import java.sql.*;
import java.util.Properties;

public class DatabaseManager {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);

    // 🔹 Database Credentials (Replace with actual values)
    private static final String DB_URL = "jdbc:postgresql://ep-fancy-term-a1ddi401-pooler.ap-southeast-1.aws.neon.tech/fraud_detection_db?sslmode=require";
    private static final String DB_USER = "Banking_owner";
    private static final String DB_PASSWORD = "npg_NPTmM9bdCoZ5";

    // 🔹 Save fraudulent transactions to PostgreSQL
    public static void saveFraudTransactions(Dataset<Row> flagged) {
        logger.info("Saving flagged fraud transactions to PostgreSQL...");

        // 🔹 Filter ONLY suspicious transactions before saving
        Dataset<Row> fraudTransactions = flagged.filter(col("is_suspicious").equalTo(true));

        if (fraudTransactions.isEmpty()) {
            logger.info("No fraudulent transactions to save.");
            return;
        }

        Properties connectionProps = new Properties();
        connectionProps.put("user", DB_USER);
        connectionProps.put("password", DB_PASSWORD);
        connectionProps.put("driver", "org.postgresql.Driver");

        String outputTablename = "fraud_transactions";

        try {
            fraudTransactions.write()
                    .option("header", "true")
                    .mode(SaveMode.Overwrite)
                    .jdbc(DB_URL, outputTablename, connectionProps);

            logger.info("Fraud transactions stored in PostgreSQL.");
        } catch (Exception e) {
            logger.error("Error writing to DB: {}", e.getMessage());
        }
    }

    public static void insertFraudLog(String level, String message, String accountId, String transactionId, String reason) {
        String insertSQL = "INSERT INTO fraud_logs (log_level, log_message, account_id, transaction_id, fraud_reason) VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(insertSQL)) {

            stmt.setString(1, level);
            stmt.setString(2, message);
            stmt.setString(3, accountId);
            stmt.setString(4, transactionId);
            stmt.setString(5, reason);
            stmt.executeUpdate();

            logger.info("Log inserted successfully for transaction ID: {}", transactionId);
        } catch (SQLException e) {
            logger.error("Failed to insert log: {}", e.getMessage());
        }
    }
}
