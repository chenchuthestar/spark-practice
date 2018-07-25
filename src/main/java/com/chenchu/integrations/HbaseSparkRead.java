package com.chenchu.integrations;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HbaseSparkRead {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[*]").appName("HbaseSparkRead")
				.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
				.getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		jsc.setLogLevel("WARN");
		String catalog = "{ \"table\":{\"namespace\":\"default\", \"name\":\"emp\"}, \"rowkey\":\"id\",\"columns\":{ \"id\":{\"cf\":\"rowkey\", \"col\":\"id\", \"type\":\"string\"}, \"name\":{\"cf\":\"personal data\", \"col\":\"city\", \"type\":\"string\"},\"age\":{\"cf\":\"personal data\", \"col\":\"name\", \"type\":\"string\"} 	 }     }";

		Map<String, String> map = new HashMap<String, String>();
		map.put("hbase.zookeeper.quorum", "localhost");
		map.put("hbase.zookeeper.property.clientPort", "2181");
		map.put("spark.hbase.host", "localhost");
		map.put(HBaseTableCatalog.tableCatalog(), catalog);
		Dataset<Row> load = spark.read().options(map).format("org.apache.spark.sql.execution.datasources.hbase").load();
		load.show();
	}

	private void sparkDataSetWriteToHbase() {

	}
}