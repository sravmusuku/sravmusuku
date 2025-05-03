package com.org.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Account {

    public static Dataset<Row> readTransactions(SparkSession spark, String path) {
        return spark.read()
                .json(path);
    }
}

