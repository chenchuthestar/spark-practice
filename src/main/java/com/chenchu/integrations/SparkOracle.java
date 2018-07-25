package com.chenchu.integrations;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkOracle {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[*]").appName("spark-oracle-connector")
				.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
				.getOrCreate();

		Properties prop = new java.util.Properties();
		prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
		prop.setProperty("user", "system");
		prop.setProperty("password", "chenchu");

		// read data from mysql
		Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:oracle:thin:@localhost:1521:xe", "student", prop);
		jdbcDF.show();

		// write data to mysql
		jdbcDF.write().jdbc("dbc:oracle:thin:@localhost:1521:xe", "student1", prop);

	}

}
