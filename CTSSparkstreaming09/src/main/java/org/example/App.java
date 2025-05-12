package org.example;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.streaming.Trigger;

/* End Region imports */
public class App {
    public static void main(String[] args) {
        // Set Hadoop home for winutils
        System.setProperty("hadoop.home.dir", "C:\\spark\\hadoop");
        //Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkETLDemo")
                .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
                .config("spark.hadoop.io.native.lib", "false")
                .master("local[*]")
                .getOrCreate();
        try {
            Dataset<Row> liveData = spark.readStream()
                    .format("rate")
                    .option("rowsPerSecond", 1)
                    .load();
            System.out.println("Streaming Started " + liveData.isStreaming());
            //Dataset<Row> wordCount = liveData.groupBy("value").count();
            liveData.writeStream().outputMode("append")
                    .format("console")
                    .trigger(Trigger.ProcessingTime("2 seconds"))
                    .option("forceDeleteTempCheckpointLocation", "true")
                    .option("checkpointLocation", "file:///C:/tmp")
                    .start()
                    .awaitTermination();


        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

    }
}
