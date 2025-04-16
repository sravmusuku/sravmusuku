package sparkPrograms;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {

		
		List<Integer> inputdata= new ArrayList<>();
		inputdata.add(35);
		inputdata.add(12);
		inputdata.add(90);
		inputdata.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf= new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
		JavaSparkContext sc= new JavaSparkContext(conf);
		JavaRDD<Integer> myRdd= sc.parallelize(inputdata);
		
		Integer result=myRdd.reduce((value1,value2)->value1+value2);
		JavaRDD<Double> sqrt= myRdd.map((value)->Math.sqrt(value));
		System.out.println(result);
		sqrt.foreach(value->System.out.println(value));
		sc.close();
		
		
		
		
	}

}
