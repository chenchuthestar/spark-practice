package com.chenchu.streaming;

import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class TextFileInputStream {
	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local[*]")
				.appName("spark-streaming textinputstream wordcounk example ").getOrCreate();
		sparkSession.sparkContext().setLogLevel("OFF");
		JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
				Durations.seconds(5));

		JavaDStream<String> tis = jssc.textFileStream("hdfs://localhost:9000/textFileStream/");

		JavaPairDStream<String, Integer> wordCounts = tis.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
				.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);

		wordCounts.print();
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}