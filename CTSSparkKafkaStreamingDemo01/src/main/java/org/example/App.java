package org.example;



import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.functions.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.*;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.streaming.Trigger;



public class App
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkStreamingDemo")
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
                .master("local[*]")
                .getOrCreate();
        try
        {
//            StructType customer_schema = new StructType()
//                    .add("customer_id",DataTypes.IntegerType)
//                    .add("customer_fullname",DataTypes.StringType)
//                    .add("email",DataTypes.StringType)
//                    .add("phone",DataTypes.StringType)
//                    .add("balance",DataTypes.IntegerType);

            Dataset<Row> liveData = spark.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers","10.0.0.15:9092")
                    .option("subscribe","citizensbanktransaction")
                    .option("startingOffsets","earliest")//other options are
                    .option("partition",0)
                    .option("failOnDataLosS","true")
                    .load()
                    .selectExpr("CAST(value as STRING) as Message");

            System.out.println("Streaming Started:" +liveData.isStreaming());


            Dataset<Row> transaformedmessageDS=liveData.withColumn("Modified_message",functions.col("Message"));

            transaformedmessageDS.writeStream()
                    .outputMode("append")
                    .format("console")
                    .option("truncate","false")
                    //.queryName("fast_stream")
                    .trigger(Trigger.ProcessingTime("1 second"))
                    .option("CheckpointLocation","tmp")
                    .start()
                    .awaitTermination();

        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
    }
}