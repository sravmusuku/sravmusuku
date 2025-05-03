package com.org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class FraudDetector {

    public static Dataset<Row> detectFraud(SparkSession spark, Dataset<Row> customers, Dataset<Row> transactions) {

        // ✅ Fix: UDF for High-Value Transactions (Null Handling)
        spark.udf().register("isHighAmount", (Double amount) -> amount != null && amount > 10000, 
                org.apache.spark.sql.types.DataTypes.BooleanType);

        // ✅ Fix: UDF for Fraud Reason
        spark.udf().register("getReason", (Double amount) -> amount != null && amount > 10000 ? "High amount alert" : "", 
                org.apache.spark.sql.types.DataTypes.StringType);

        // ✅ Fix: Use `timestamp` Instead of `transaction_time`
        WindowSpec windowSpec = Window.partitionBy("account_id").orderBy("timestamp");

        // ✅ Join Customers & Transactions on `account_id`
        Dataset<Row> joined = transactions
                .join(customers, "account_id");

        // ✅ Add Fraud Detection Columns
        Dataset<Row> flagged = joined
                .withColumn("is_suspicious", callUDF("isHighAmount", col("amount").cast("double")))
                .withColumn("reason", callUDF("getReason", col("amount").cast("double")))

                // Fix: Using `timestamp` Instead of `transaction_time`
                .withColumn("prev_transaction_time", lag("timestamp", 1).over(windowSpec))
                .withColumn("time_diff", unix_timestamp(coalesce(col("timestamp"), lit("1970-01-01 00:00:00")))
                        .minus(unix_timestamp(coalesce(col("prev_transaction_time"), lit("1970-01-01 00:00:00")))))
                .withColumn("is_multiple_transactions", when(col("time_diff").leq(180), true).otherwise(false));

        return flagged;
    }
}
