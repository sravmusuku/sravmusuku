package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Customers {
    public static Dataset<Row> readCustomers(SparkSession spark, String path) {
        return spark.read()
                .option("multiline", "true")
                .json(path)
                .withColumnRenamed("status", "customer_status");
    }
}
