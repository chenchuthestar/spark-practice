package com.chenchu.streaming;

import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class NetworkWordCount {
	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local[*]")
				.appName("spark-streaming nework wordcounk example ").getOrCreate();
		sparkSession.sparkContext().setLogLevel("OFF");
		JavaStreamingContext ssc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
				Durations.seconds(5));

		JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 11111);
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
				.reduceByKey((i1, i2) -> i1 + i2);
		wordCounts.print();
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}