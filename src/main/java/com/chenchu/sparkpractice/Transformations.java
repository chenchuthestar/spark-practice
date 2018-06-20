package com.chenchu.sparkpractice;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Transformations {
	static SparkSession ss = SparkSession.builder().appName("spark-practice").master("local[*]").getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
	static JavaRDD<Integer> irdd = jsc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9));
	
	public static void main(String[] args) {
		jsc.setLogLevel("OFF");
		System.out.println(ss);
		System.out.println(jsc);
		JavaRDD<Integer> mapRdd = irdd.map(x -> x+1);
		System.out.println(mapRdd.collect());

		
	}
}
