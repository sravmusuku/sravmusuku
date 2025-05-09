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



public class App
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkStreamingDemo")
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                .config("spark.driver.memory", "2g")
                .config("spark.driver.cores", "2")
                .config("spark.executor.memory", "4g")
                .config("spark.executor.cores", "2")
                .master("local[*]")
                .getOrCreate();
        try
        {
            StructType customer_schema = new StructType()
                    .add("customer_id",DataTypes.IntegerType)
                    .add("customer_fullname",DataTypes.StringType)
                    .add("email",DataTypes.StringType)
                    .add("phone",DataTypes.StringType)
                    .add("balance",DataTypes.IntegerType);

            Dataset<Row> liveData = spark.readStream()
                    .format("csv")
                    .option("header","true")
                    .option("maximumPerTrigger",1)
                    .schema(customer_schema)
                    .load("input_files");

            System.out.println("Streaming Started:" +liveData.isStreaming());
            liveData.printSchema();

            Dataset<Row> cleanedData =liveData.dropDuplicates();

            String pgconnectionUrl = "jdbc:postgresql://ep-delicate-fog-a1i3vqh5-pooler.ap-southeast-1.aws.neon.tech/banking?sslmode=require";
            //postgresql://banking_owner:npg_vEof3I9kFxzn@ep-delicate-fog-a1i3vqh5-pooler.ap-southeast-1.aws.neon.tech/banking?sslmode=require
            Properties connectionProps = new Properties();
            connectionProps.put("user","banking_owner");
            connectionProps.put("password","npg_vEof3I9kFxzn");
            connectionProps.put("driver","org.postgresql.Driver");
            String output_tablename = "Enriched_customers";

            cleanedData.writeStream()
                    .foreachBatch((batchds,batchid)->{
                        batchds.write()
                                .mode("append")
                                .jdbc(pgconnectionUrl, output_tablename, connectionProps);

                    })
                    .outputMode(OutputMode.Append())
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