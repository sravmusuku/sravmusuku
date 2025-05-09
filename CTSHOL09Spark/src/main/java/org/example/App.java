package org.example;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

public class App 
{
    public static void main( String[] args )
    {
        SparkSession spark=SparkSession.builder()
                .appName("JavaSparkETLPipeline")
                .master("local[*]")
                .getOrCreate();

        StructType schema=new StructType()
                .add("id",DataTypes.IntegerType)
                .add("name",DataTypes.StringType)
                .add("age",DataTypes.IntegerType)
                .add("salary",DataTypes.DoubleType)
                .add("department",DataTypes.StringType);

        Dataset<Row> csvData=spark.read()
                .option("header","true")
                .schema(schema)
                .csv("employees.csv");
        Dataset<Row> filteredData=csvData.filter("salary>50000");
        Dataset<Row> selectedData=filteredData.select("id","name","salary","department");
        Dataset<Row> aggregatedData=selectedData.groupBy("department")
                .agg(functions.avg("salary").alias("avg_salary"));
         aggregatedData.show();
        aggregatedData.write()
                .mode(SaveMode.Overwrite)
                .parquet("output/employees_parquet");
            spark.stop();

    }
}
