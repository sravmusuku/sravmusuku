package org.example;

/* Region imports */

import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.Trigger.*;
import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/* End Region imports */

public class App

{

    public static void main( String[] args )

    {

        //Create Spark Session

        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkETLDemo")
                .master("local[*]")
                .getOrCreate();

        try {

            Dataset<Row> liveData = spark.readStream()
                    .format("socket")
                    .option("host","localhost")
                    .option("port",9999)
                    .load();

            System.out.println("Socket Streaming Started : "+liveData.isStreaming());
            Dataset<Row> customerDS= liveData.select(functions.explode(functions.split(functions.col("value"), " "))
                    .alias("customer_name"));
            Dataset<Row> wordCount = customerDS.groupBy("customer_name").count();
            wordCount.writeStream().outputMode("complete")
                    .format("console")

                    //.trigger(Trigger.ProcessingTime("2 seconds"))

                    .option("forceDeleteTempCheckpointLocation", "true")
                    .option("checkpointLocation","temp")

                    //.trigger(Trigger.ProcessingTime(5000))

                    .trigger(Trigger.ProcessingTime("2 seconds"))
                    .start()
                    .awaitTermination();


        }catch (Exception ex)

        {

            System.out.println(ex.getMessage());

        }

    }

}
