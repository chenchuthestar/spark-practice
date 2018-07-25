package com.chenchu.spark_core;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class BroadCastVariables {
	static SparkSession ss = SparkSession.builder().appName("spark-practice").master("local[*]")
			.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
			.getOrCreate();
	static JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
	static Broadcast<List<String>> broadcast = jsc.broadcast(stopWords());

	public static void main(String[] args) {
		try {
			jsc.setLogLevel("OFF");
			JavaRDD<String> textFile = jsc.textFile("file:///home/orienit/myfile");
			List<String> list = broadcast.value();
			JavaRDD<String> filter = textFile.filter(str -> {
				Boolean status = Boolean.FALSE;
				for (String s : list) {
					if (str.contains(s)) {
						status = Boolean.TRUE;
						continue;
					}
				}
				return status;
			});
			System.out.println(filter.collect());
			while (true) {

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static List<String> stopWords() {
		List<String> asList = Arrays.asList("chenchu", "the", "star");
		return asList;
	}

}
