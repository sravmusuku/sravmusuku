package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;

public class App
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder()
                .appName("CTSSparkDemo")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        try {
            List<String> names = Arrays.asList("John,25", "Jack,45", "James,56", "David,23");
            JavaRDD<String> rdd = sc.parallelize(names);
            JavaRDD<String> filteredRDD = rdd.filter(name -> {
                String[] filteredNames = name.split(",");
                int age = Integer.parseInt(filteredNames[1]);
                return age > 25;
            });
            System.out.println("Age greater than 25:");
            filteredRDD.collect().forEach(System.out::println);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            // Stop Spark session
            spark.stop();
        }
    }
}