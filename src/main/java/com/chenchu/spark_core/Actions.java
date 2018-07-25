package com.chenchu.spark_core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Actions {
	static SparkSession ss = SparkSession.builder().appName("spark-practice").master("local[*]")
			.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
			.getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
	static JavaRDD<Integer> irdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
	static JavaRDD<String> sRdd = jsc
			.parallelize(Arrays.asList("I am going", "to hyd", "I am learning", "hadoop course"));

	static JavaPairRDD<String, Integer> mapToPair = sRdd
			.mapToPair(str -> new Tuple2<String, Integer>(str, str.length()));

	public static void main(String[] args) {

		jsc.setLogLevel("OFF");

		// 1.) count()
		long count = irdd.count();
		System.out.println(count);

		// 2.) collect()
		List<Integer> collect = irdd.collect();
		System.out.println(collect);

		// 3.) take
		List<String> take = sRdd.take(2);
		System.out.println(take);

		// 4.) top()
		List<String> top = sRdd.top(2);
		System.out.println(top);

		// 5.) countByValue()
		Map<Tuple2<String, Integer>, Long> countByValue = mapToPair.countByValue();
		System.out.println(countByValue);

		// 6.) reduce()
		Integer reduce = irdd.reduce((i1, i2) -> (i1 + i2));
		System.out.println(reduce);

		// 7.) fold()
		Integer fold = irdd.fold(0, ((i1, i2) -> (i1 + i2)));
		System.out.println(fold);

		// 8.) aggregate
		Integer aggregate = irdd.aggregate(0, ((i1, i2) -> (i1 + i2)), (i1, i2) -> (i1 + i2));
		System.out.println(aggregate);

		// 9.) foreach
		irdd.collect().forEach(System.out::println);

		// 10.) first()
		Integer first = irdd.first();
		System.out.println(first);

		// 11.)takeSample()
		List<Integer> takeSample = irdd.takeSample(false, 3);
		System.out.println(takeSample);

		// 12.) takeOrdered
		List<Integer> takeOrdered = irdd.takeOrdered(5);
		System.out.println(takeOrdered);

		// 13.) saveAsTextFile
		irdd.saveAsTextFile("/home/orienit/op-text");

		// 14.) saveAsSequenceFile
		// mapToPair.saveAsSequenceFile("/home/orienit/op-seq");

		// 15.) saveAsObjectFile
		mapToPair.saveAsObjectFile("/home/orienit/op-obj");

		// 16.) countByKey
		Map<String, Long> countByKey = mapToPair.countByKey();
		System.out.println(countByKey);

		// 17.) glom()
		JavaRDD<List<Integer>> glom = irdd.glom();
		System.out.println(irdd.count() + "     " + glom.count());

	}

}
