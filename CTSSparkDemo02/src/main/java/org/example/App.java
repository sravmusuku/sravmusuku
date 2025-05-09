package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;

public class App
{
    public static void main( String[] args )
    {
        // Initialize Spark
        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkDemo")
                .master("local[*]")  // Use local mode with all cores
                .getOrCreate();

        //Create the Spark context on Driver
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        try {
            // Sample data processing
            // Data collection (Extraction)
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 7, 10, 10, 19, 20, 25, 56, 67, 89);
            JavaRDD<Integer> rdd = sc.parallelize(data);
            System.out.println("Number of partitions:"+rdd.getNumPartitions());
            System.out.println("Number of Elements:"+rdd.count());

            // Calculate sum (Transformations)
            int sum = rdd.reduce(Integer::sum);
            System.out.println("Sum of numbers: " + sum);

            // Calculate squares (Load)
            JavaRDD<Integer> squares = rdd.map(x -> x * x);
            System.out.println("Squares: " + squares.collect()); //Load

        }catch(Exception ex) {

            System.out.println(ex.getMessage());

        } finally {
            // Stop Spark session
            spark.stop();
        }

    }
}