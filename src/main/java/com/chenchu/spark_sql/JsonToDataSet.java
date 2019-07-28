package com.chenchu.spark_sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonToDataSet {

	static SparkSession ss = SparkSession.builder().appName("spark-practice")
			.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
			.master("local[*]").getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

	public static void main(String[] args) {
		jsonToDataSet();
	}

	private static void jsonToDataSet() {
		jsc.setLogLevel("OFF");
		Dataset<Row> dataset = ss.read().json("student.json");
		dataset.show();
	}
}
