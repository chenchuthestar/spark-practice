package com.chenchu.streaming;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import scala.Tuple2;

public class JavaStructuredNetworkWordCountWindowed {
	public static void main(String[] args) {

		String host = "localhost"; // 1->host,2->port,3-> window size,4-> slide size
		int port = 9999;
		int windowSize = Integer.parseInt("5");
		int slideSize = 5;// (args.length == 3) ? windowSize : Integer.parseInt(args[3]);
		if (slideSize > windowSize) {
			System.err.println("<slide duration> must be less than or equal to <window duration>");
		}
		String windowDuration = windowSize + " seconds";
		String slideDuration = slideSize + " seconds";

		SparkSession spark = SparkSession.builder().master("local[*]").appName("JavaStructuredNetworkWordCountWindowed")
				.getOrCreate();
		spark.sparkContext().setLogLevel("OFF");
		Dataset<Row> lines = spark.readStream().format("socket").option("host", host).option("port", port)
				.option("includeTimestamp", true).load();

		Dataset<Row> words = lines.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
				.flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
					List<Tuple2<String, Timestamp>> result = new ArrayList<>();
					for (String word : t._1.split(" ")) {
						result.add(new Tuple2<>(word, t._2));
					}
					return result.iterator();
				}, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF("word", "timestamp");

		Dataset<Row> windowedCounts = words
				.groupBy(functions.window(words.col("timestamp"), windowDuration, slideDuration), words.col("word"))
				.count().orderBy("window");
		StreamingQuery query = windowedCounts.writeStream().outputMode("complete").format("console")
				.option("truncate", "false").start();

		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
	}
}
