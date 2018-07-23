package com.chenchu.spark_core;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class PairRddExamples {

	static SparkSession ss = SparkSession.builder().appName("spark-practice").master("local[*]").getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
	static JavaRDD<Integer> irdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
	static JavaRDD<String> sRdd = jsc
			.parallelize(Arrays.asList("I am going", "to hyd", "I am learning", "hadoop course"));

	public static void main(String[] args) {
		JavaRDD<String> flatMap = sRdd.flatMap(str -> Arrays.asList(str.split(" ")).iterator());
		JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(str -> new Tuple2<String, Integer>(str, 1));

		// aggregateByKey
		JavaPairRDD<String, Integer> aggregateByKey = mapToPair.aggregateByKey(0, ((i1, i2) -> (i1 + i2)),
				(i1, i2) -> (i1 + i2));
		aggregateByKey.collect().forEach(tup -> System.out.println(tup._1() + "   " + tup._2()));

		// groupBykey
		JavaPairRDD<String, Iterable<Integer>> groupByKey = mapToPair.groupByKey();

		// reducebykey
		JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey((i1, i2) -> (i1 + i2));
		reduceByKey.collect().forEach(tup -> System.out.println(tup._1() + "   " + tup._2()));

	}
}
