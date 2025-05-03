package org.example;
import java.util.*;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.SaveMode;
public class App
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkDemo")
                .master("local[*]")
                .getOrCreate();
        try {
            String input_file =  "transaction_data.csv";
            Dataset<Row> transactionds = spark.read()
                    .option("header","true")
                    .option("inferSchema", "true")
                    .csv(input_file);
            transactionds.createOrReplaceTempView("transactions");

            Dataset<Row> mobiletransactionds = spark.sql("select * from transactions where `device used` = 'Mobile'");
            mobiletransactionds.printSchema();

            List<Row> mobiletrans_records =  mobiletransactionds.takeAsList(10);
            for(Row row:mobiletrans_records) {
                System.out.println(row);
            }

            Row firstRow = mobiletransactionds.first();
            System.out.println(firstRow);

            //Drop all the records which contains null values in any column of dataset
            // Dataset<Row> cleaned_mobiletransactionds = mobiletransactionds.na().drop();
            System.out.println("Total number of mobile transaction records : "+mobiletransactionds.count());

            //Drop all the records which contains null values in Transaction ID of dataset
            Dataset<Row> cleaned_mobiletransactionds = mobiletransactionds.na().drop(new String[]{"Transaction ID"});
            System.out.println("Total number of transaction record after removing null records : "+cleaned_mobiletransactionds.count());

            //removing duplicates
            cleaned_mobiletransactionds = cleaned_mobiletransactionds.dropDuplicates();
            System.out.println("Total number of transaction record after removing duplicates : "+cleaned_mobiletransactionds.count());
            //Renaming a column.
            cleaned_mobiletransactionds = cleaned_mobiletransactionds.withColumnRenamed("Sender Account ID","SenderAccount")
                    .withColumnRenamed("Transaction ID","TransactionId")
                    .withColumnRenamed("Transaction Amount","TransactionAmount")
                    .withColumnRenamed("Transaction Type","TransactionType");
            cleaned_mobiletransactionds.createOrReplaceTempView("mobile_transactions");

            cleaned_mobiletransactionds.printSchema();
            Dataset<Row> transaction1kds = spark.sql("select TransactionId, SenderAccount, TransactionAmount, TransactionType, Timestamp from mobile_transactions where TransactionAmount > 1000 order by Timestamp ");
            transaction1kds.show();

            //Store the dataset into postgres database.
            String pgconnectionUrl = "jdbc:postgresql://ep-delicate-fog-a1i3vqh5-pooler.ap-southeast-1.aws.neon.tech/banking?sslmode=require";
            //postgresql://banking_owner:npg_vEof3I9kFxzn@ep-delicate-fog-a1i3vqh5-pooler.ap-southeast-1.aws.neon.tech/banking?sslmode=require
            Properties connectionProps = new Properties();
            connectionProps.put("user","banking_owner");
            connectionProps.put("password","npg_vEof3I9kFxzn");
            connectionProps.put("driver","org.postgresql.Driver");

            String output_tablename = "Enriched_Transactions";

            transaction1kds.write()
                    .option("header","true")
                    .mode(SaveMode.Overwrite)
                    .jdbc(pgconnectionUrl, output_tablename, connectionProps);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            spark.stop();
        }
    }
}