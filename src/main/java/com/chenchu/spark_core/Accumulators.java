package com.chenchu.spark_core;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

public class Accumulators {

	static SparkSession ss = SparkSession.builder().appName("accumulators-test").master("local[*]")
			.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
			.getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());

	public static void main(String[] args) {
		LongAccumulator longAccumulator = jsc.sc().longAccumulator("mulong");
		longAccumulator.add(1);
		longAccumulator.add(1);
		longAccumulator.add(2);
		Long value = longAccumulator.value();
		System.out.println(value);
		System.out.println(longAccumulator.sum());
		System.out.println(longAccumulator.avg());
		while (true) {

		}
	}

}
