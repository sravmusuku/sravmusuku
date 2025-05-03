package com.org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Customer {

    public static Dataset<Row> readCustomers(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .csv(path);
    }
}

