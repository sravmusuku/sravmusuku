package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class FraudDetector {

    public static Dataset<Row> detectFraud(SparkSession spark, Dataset<Row> customers, Dataset<Row> transactions) {

        // ✅ Register UDF for High-Value Transactions
        spark.udf().register("isHighAmount", (UDF1<Double, Boolean>) amount -> amount != null && amount > 10000,
                org.apache.spark.sql.types.DataTypes.BooleanType);

        // ✅ Register UDF for Fraud Reason (High-Value Transaction)
        spark.udf().register("getReason", (UDF1<Double, String>) amount -> amount != null && amount > 10000 ? "High Value Transaction" : "",
                org.apache.spark.sql.types.DataTypes.StringType);

        // ✅ Rename `status` column in customers to prevent ambiguity
        Dataset<Row> customersRenamed = customers.withColumnRenamed("status", "customer_status");

        // ✅ Define Window for Rapid Transactions Detection (partitioned by account_id)
        WindowSpec windowSpec = Window.partitionBy("account_id").orderBy("timestamp");

        // ✅ Identify rapid transactions using a rolling window function
        transactions = transactions
                .withColumn("prev_transaction_time", lag("timestamp", 1).over(windowSpec))
                .withColumn("time_diff", unix_timestamp(col("timestamp"))
                        .minus(unix_timestamp(col("prev_transaction_time"))))
                .withColumn("transaction_count_in_window", count("transaction_id").over(
                        Window.partitionBy("account_id").orderBy("timestamp")
                                .rowsBetween(-1, 0)))  // ✅ Tracks recent transactions
                .withColumn("isRapidTransaction", when(col("transaction_count_in_window").gt(1), true).otherwise(false));

        // ✅ Join Customers & Transactions on `account_id`
        Dataset<Row> enrichedTransactions = transactions.join(customersRenamed, "account_id");

        // ✅ Select required columns & apply fraud detection
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
                        col("status"),
                        when(col("isRapidTransaction").or(callUDF("isHighAmount", col("amount").cast("double"))), true)
                                .otherwise(false).alias("is_suspicious"),
                        when(col("isRapidTransaction"), "Rapid Transaction")
                                .otherwise(when(callUDF("isHighAmount", col("amount").cast("double")), "High Value Transaction")
                                        .otherwise("Normal Transaction")).alias("reason")  // ✅ Corrected version

                );

        return flagged;
    }
}
