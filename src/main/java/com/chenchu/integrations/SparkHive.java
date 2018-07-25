package com.chenchu.integrations;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHive {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[*]").appName("spark-hive")
				.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
				.enableHiveSupport().getOrCreate();

		Properties prop = new java.util.Properties();
		prop.setProperty("driver", "org.apache.hive.jdbc.HiveDriver");
		prop.setProperty("user", "orienit");
		prop.setProperty("password", "");
		Dataset<Row> jdbcDF = spark.read().format("org.apache.hive.jdbc.HiveDriver")
				.jdbc("jdbc:hive2://localhost:10000/kalyan", "student", prop);
		jdbcDF.show();

	}

}
