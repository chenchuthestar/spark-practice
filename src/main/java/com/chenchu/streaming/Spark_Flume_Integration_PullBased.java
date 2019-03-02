package com.chenchu.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class Spark_Flume_Integration_PullBased {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("spark session example")
				.getOrCreate();
		sparkSession.sparkContext().setLogLevel("OFF");
		JavaStreamingContext ssc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
				Durations.seconds(5));
		JavaReceiverInputDStream<SparkFlumeEvent> stream = FlumeUtils.createPollingStream(ssc, "localhost", 4445);
		JavaDStream<String> map = stream.map(sfe -> {
			System.out.println("inside");
			return new String(sfe.event().getBody().array());
		});

		map.print();
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
