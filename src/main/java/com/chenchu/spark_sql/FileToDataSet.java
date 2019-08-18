package com.chenchu.spark_sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FileToDataSet {

	static SparkSession ss = SparkSession.builder().appName("spark-practice")
			.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
			.master("local[*]").getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

	public static void main(String[] args) {
		jsc.setLogLevel("OFF");
		// jsonToDataSet();
		csvTODataSet();
	}

	private static void jsonToDataSet() {
		// 1st way
		Dataset<Row> dataset1 = ss.read().json("src/main/resources/student.json");
		dataset1.show();

		// second way
		Dataset<Row> dataset2 = ss.read().format("json").load("src/main/resources/student.json");
		dataset2.show();
	}

	private static void csvTODataSet() {
		// 1st way
		Dataset<Row> dataset = ss.read().option("header", "true").csv("src/main/resources/student.csv");
		dataset.show();
		// second way
		Dataset<Row> dataset2 = ss.read().option("header", "true").format("csv").load("src/main/resources/student.csv");
		dataset2.show();
	}
}
