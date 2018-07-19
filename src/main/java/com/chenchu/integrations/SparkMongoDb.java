package com.chenchu.integrations;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkMongoDb {

	public static void main(String[] args) {
		SparkSession ss = SparkSession.builder().appName("mongodb-connector").master("local[*]").getOrCreate();

		Map<String, String> map = new HashMap<String, String>();
		map.put("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017");
		map.put("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017");
		map.put("collection", "student");
		map.put("database", "chenchu");
		Dataset<Row> load = ss.read().format("com.mongodb.spark.sql.DefaultSource").options(map).load();
		load.show();

	}

}
