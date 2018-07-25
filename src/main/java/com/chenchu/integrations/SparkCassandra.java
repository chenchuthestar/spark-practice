package com.chenchu.integrations;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkCassandra {
	public static void main(String[] args) {
		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("keyspace", "kalyan_cql");
		map.put("spark.cassandra.connection.host", "localhost");
		map.put("spark.cassandra.connection.port", "9042");
		map.put("table", "student");
		map.put("spark.cassandra.auth.username", "");
		map.put("spark.cassandra.auth.password", "");
		SparkSession ss = SparkSession.builder().appName("cassandra-connector")
				.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
				.master("local[*]").getOrCreate();
		Dataset<Row> dataset = ss.read().format("org.apache.spark.sql.cassandra").options(map).load();
		dataset.show();

	}

}
