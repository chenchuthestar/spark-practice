package com.chenchu.spark_sql.dsl;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class DataSet_DSL {
	static SparkSession ss = SparkSession.builder().appName("spark-practice")
			.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
			.master("local[*]").getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
	static Dataset<Row> dataset = ss.read().json("src/main/resources/student.json");

	public static void main(String[] args) {
		jsc.setLogLevel("OFF");
		dataset.show();
		// select example
		dataset.select("name", "id").show();
		dataset.select(dataset.col("name"), dataset.col("id")).show();

		// selectexpr example
		dataset.selectExpr("name", "id*10").show();

		// filter and where functions
		// filter is alias for where (both are same)
		dataset.filter("id > 4 and id < 6").show();
		dataset.where("id > 2").show();

		// limit function
		dataset.limit(4).show();

		// toJson function
		dataset.toJSON().show();

		// groupBy example
		dataset.groupBy("name").count().show();

		// orderBy example
		dataset.orderBy("year").show();

		// sort by example
		dataset.sort("course", "id").show();
		// agg examples
		dataset.agg(min("year")).show();
		// describe
		dataset.describe("year").show();
		// distinct
		dataset.distinct().show();
		//drop
		dataset.drop("name").show();
		
		//dropDuplicates
		dataset.dropDuplicates().show();
		// select
		dataset.select("course").show(); 
		// withColumn
		Column newCol = when(col("course").equalTo("spark"), "i like this tech")
			    .when(col("course").equalTo("hadoop"), "i don't like this tech")
			    .otherwise("not intrested");
		dataset.withColumn("my openion", newCol).show();
	}
}
