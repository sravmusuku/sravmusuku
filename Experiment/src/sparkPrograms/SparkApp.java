package sparkPrograms;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkApp {
    public static void main(String[] args) {
        // Step 1: Create Spark Configuration
        SparkConf conf = new SparkConf()
                .setAppName("SimpleSparkApp")
                .setMaster("local[*]"); // Run locally with all available cores

        // Step 2: Create Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Step 3: Load a text file (Make sure the file exists at this path)
        JavaRDD<String> lines = sc.textFile("src/sparkPrograms/sample.txt");
        JavaRDD<String> words=lines.flatMap(line->Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> wordpairs= words.mapToPair(word-> new scala.Tuple2<>(word,1));
        JavaPairRDD<String,Integer> wordcounts=wordpairs.reduceByKey((a,b)->a+b);
        wordcounts.foreach(pair->System.out.println(pair._1()+":"+pair._2()));

        // Step 4: Count the number of lines in the file
        //long lineCount = lines.count();

        // Step 5: Print the result
        //System.out.println("Number of lines in the file: " + lineCount);

        // Step 6: Stop Spark Context
        sc.close();
    }
}
