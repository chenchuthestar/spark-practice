package com.chenchu.spark_sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SqlContextExample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("sqlcontext example").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		Dataset<Row> json = sqlContext.read().json("src/main/resources/student.json");
		json.show();
	}
}