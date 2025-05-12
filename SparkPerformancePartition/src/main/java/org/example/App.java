package org.example;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class App
{
    public static void main( String[] args )
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkETLDemo")
                .config("spark.sql.files.maxPartitionBytes", "3MB")
                //.config("spark.sql.shuffle.partitions", "4")
                .master("local[*]")
                .getOrCreate();
        try
        {
            String input_filename = "transaction_data.csv";
            Dataset<Row> transactionds = spark.read()
                    .option("header","true")
                    .option("inferSchema","true")
                    .csv(input_filename);
            System.out.println("Total number of columns:" +transactionds.columns().length);
            transactionds.show();
            transactionds.printSchema();

            System.out.println("Total number of actual records:" +transactionds.count());

            transactionds.createOrReplaceTempView("transactions"); //virtual table  in spark cluster memory
            Dataset<Row> mobiletransactionds = spark.sql("select * from transactions where `device used` = 'Mobile' ");
            mobiletransactionds.show(15);
            transactionds.head(10); // Top 10 records will be fetched by using this
            System.out.println("Total number of mobile transactions records:" +mobiletransactionds.count());


            try {
                //Loading the data
                System.out.println("Max Partition bytes: "+spark.conf().get("spark.sql.files.maxPartitionBytes"));
                //mobiletransactionds.repartition(2); // Repartition a method to increase or decrease the number of partition in a RDD
                mobiletransactionds.coalesce(2);
                System.out.println("Number of Partition in Mobile Transactions RDD: "+mobiletransactionds.rdd().getNumPartitions());

                mobiletransactionds.write().mode("overwrite").parquet("output/curratedcustomers");
                transactionds.write().mode("overwrite").option("maxRecordsPerFile", 100).partitionBy("Transaction Type").parquet("output/transactions");
                transactionds.write().mode("overwrite").option("maxRecordsPerFile",50).partitionBy("Transaction Type").csv("output/csv/transactions");

                input_filename = "output\\transactions\\Transaction%20Type=Deposit";
                Dataset<Row> depositds = spark.read()
                        .option("inferSchema","true")
                        .parquet(input_filename);
                System.out.println("Total number of records:" +depositds.count());
                depositds.show();

            }catch (Exception ex){
                System.out.println("Some error occured: "+ex.getMessage());
            }
        }
        catch(Exception ex)
        {

            System.out.println(ex.getMessage());
        }
        finally
        {
            spark.stop();
        }
    }
}
