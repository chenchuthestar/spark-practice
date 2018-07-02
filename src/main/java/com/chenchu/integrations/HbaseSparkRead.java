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
		/*
		 * copy hbase-site.xml file to spark/conf folder for remotely access hbase with
		 * spark
		 */
		String catalog = "{ \"table\":{\"namespace\":\"default\", \"name\":\"emp\"}, \"rowkey\":\"id\",\"columns\":{ \"id\":{\"cf\":\"rowkey\", \"col\":\"id\", \"type\":\"string\"}, \"name\":{\"cf\":\"personal data\", \"col\":\"city\", \"type\":\"string\"},\"age\":{\"cf\":\"personal data\", \"col\":\"name\", \"type\":\"string\"} 	 }     }";
		
		SparkSession spark = SparkSession.builder().master("local[*]").appName("HbaseSparkRead").getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		jsc.setLogLevel("WARN");
	/*	Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "localhost");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("spark.hbase.host", "localhost");*/
		//jsc.hadoopConfiguration().addResource(config);
		/*jsc.hadoopConfiguration().set("hbase.zookeeper.quorum", "localhost");
		jsc.hadoopConfiguration().set("hbase.zookeeper.property.clientPort", "2181");
		jsc.hadoopConfiguration().set("hbase.zookeeper.property.clientPort", "2181");*/
		Map<String, String> map = new HashMap<String, String>();
		
		//map.put(HBaseTableCatalog.table(), "emp");
		System.out.println(HBaseTableCatalog.tableCatalog());
		map.put(HBaseTableCatalog.tableCatalog(), catalog);
		Dataset<Row> load = spark.read().options(map).format("org.apache.spark.sql.execution.datasources.hbase").load();
		load.show();
	}
}