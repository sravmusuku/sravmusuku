package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
public class SparkAverageCount {
    public static void main(String[] args) {
        // Initialize Spark Configuration and Context
        // Initialize Spark
        SparkSession spark = SparkSession.builder()
                .appName("CTSDemoApp")
                .master("local[*]") // Use local mode with all cores
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Integer[] numbersArray = {10, 20, 30, 40, 50,55,48,44};
        JavaRDD<Integer> numbersRDD = sc.parallelize(java.util.Arrays.asList(numbersArray));
        long count = numbersRDD.count();
        int sum = numbersRDD.reduce(Integer::sum);
        double average = (count == 0) ? 0 : (double) sum / count;
        System.out.println("Count: " + count);
        System.out.println("Average: " + average);
        sc.close();
    }
}
