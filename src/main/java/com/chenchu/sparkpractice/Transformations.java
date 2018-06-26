package com.chenchu.sparkpractice;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Transformations {
	static SparkSession ss = SparkSession.builder().appName("spark-practice").master("local[*]").getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
	static JavaRDD<Integer> irdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
	static JavaRDD<String> sRdd = jsc
			.parallelize(Arrays.asList("I am going", "to hyd", "I am learning", "hadoop course"));

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		jsc.setLogLevel("OFF");
		System.out.println(ss);
		System.out.println(jsc);
		System.out.println("rdd partitions:" + irdd.getNumPartitions());
		// map
		JavaRDD<Integer> mapRdd = irdd.map(x -> x + 1);
		System.out.println(mapRdd.collect());

		// filter
		JavaRDD<Integer> filterRdd = irdd.filter(x -> x > 1);
		System.out.println(filterRdd.collect());

		// flatmap
		JavaRDD<String> flatmapRdd = sRdd.flatMap(str -> Arrays.asList(str.split(" ")).iterator());
		System.out.println(flatmapRdd.collect());

		// mappartitions
		JavaRDD<Integer> mapPartitionsRdd = irdd.mapPartitions(itr -> Arrays.asList(itr.next()).iterator());
		System.out.println(mapPartitionsRdd.collect());

		Function2<Integer, Iterator<Integer>, Iterator<String>> fun = (i, itr) -> {
			System.out.print(i + "    ");
			while (itr.hasNext()) {
				System.out.print(itr.next() + ",");
			}

			System.out.println();
			return Arrays.asList("Abc").iterator();
		};
		JavaRDD<String> mapPartitionsWithIndexRdd = irdd.mapPartitionsWithIndex(fun, true);
		System.out.println(mapPartitionsWithIndexRdd.collect());
		System.out.println("==============================sample() Example==========================");
		JavaRDD<Integer> sampleRdd = irdd.sample(true, 0.5);
		System.out.println(sampleRdd.collect());

		JavaRDD<Integer> sampleRdd1 = irdd.sample(false, 0.5);
		System.out.println(sampleRdd1.collect());

		System.out.println("=================union example========================================");

		JavaRDD<Integer> unionRdd = irdd.union(jsc.parallelize(Arrays.asList(9, 8, 7)));
		System.out.println(unionRdd.collect());

		System.out.println("=================intersection================================");
		JavaRDD<Integer> intersectionRdd = irdd.intersection(jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5)));
		System.out.println(intersectionRdd.collect());

		System.out.println("==================distinct=================================");

		JavaRDD<Integer> distinctRdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 4, 3)).distinct();
		System.out.println(distinctRdd.collect());

		System.out.println("===================== tuple rdd==============");
		
		JavaRDD<String> flatMap = sRdd.flatMap(str -> Arrays.asList(str.split(" ")).iterator());
		JavaPairRDD<String,Integer> mapToPair = flatMap.mapToPair(str -> new Tuple2<String,Integer>(str,1));
		JavaPairRDD<String,Iterable<Integer>> groupByKey = mapToPair.groupByKey();
		System.out.println(groupByKey.collect());
		JavaPairRDD<String,Integer> reduceByKey = mapToPair.reduceByKey((a ,b ) -> a+b );
		System.out.println(reduceByKey.collect());
		
		
		
		
		
		
		
		
		
		
		
		

	}
}
