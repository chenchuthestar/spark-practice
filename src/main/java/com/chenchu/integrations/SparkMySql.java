package com.chenchu.integrations;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkMySql {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[*]").appName("spark-mysql-connector")
				.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
				.getOrCreate();

		Properties prop = new java.util.Properties();
		prop.setProperty("driver", "com.mysql.jdbc.Driver");
		prop.setProperty("user", "root");
		prop.setProperty("password", "root");

		// read data from mysql
		Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:mysql://localhost:3306/sqoop", "student", prop);
		jdbcDF.show();

		// write data to mysql
		jdbcDF.write().jdbc("jdbc:mysql://localhost:3306/sqoop", "student1", prop);

	}

}
