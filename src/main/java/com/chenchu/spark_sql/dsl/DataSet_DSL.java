package com.chenchu.spark_sql.dsl;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSet_DSL {
	static SparkSession ss = SparkSession.builder().appName("spark-practice")
			.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
			.master("local[*]").getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
	static Dataset<Row> dataset = ss.read().json("src/main/resource/student.json");

	public static void main(String[] args) {
		jsc.setLogLevel("OFF");
		dataset.show();
		// select example
		dataset.select("name", "id").show();
		dataset.select(dataset.col("name"),dataset.col("id")).show();
	}

	

}
