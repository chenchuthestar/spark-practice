package com.chenchu.integrations;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkS3FileSystem {

	public static void main(String[] args) {
		String fileloc = "s3n://chenchuthestar/chenchu";
		SparkSession spark = SparkSession.builder().master("local[*]").appName("spark-s3-coonector")
				.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
				.getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		jsc.setLogLevel("WARN");
		jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "*******************");// "AKIAIMX6WIKC4N6SZ6JQ"
		jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "**************");
		Dataset<Row> textFile = spark.read().option("delimiter", ",").option("header", "true").csv(fileloc);
		textFile.show();

	}

}
