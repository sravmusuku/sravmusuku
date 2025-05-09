package org.example;


import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.functions.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.*;
import org.apache.spark.sql.SaveMode;


public class App
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkStreamingDemo")
                .master("local[*]")
                .getOrCreate();
        try
        {
            Dataset<Row> liveData = spark.readStream()
                    .format("socket")
                    .option("host","localhost")
                    .option("port",9999)
                    .load();
            System.out.println("Streaming Started:" +liveData.isStreaming());

            //Dataset<Row> wordCount = liveData.groupBy("value").count();
            StreamingQuery query= liveData.writeStream()
                    .outputMode("append")
                    .format("console")
                    .option("checkpointLocation","C://temp")
                    .start();
            query.awaitTermination(60000);

        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
    }
}