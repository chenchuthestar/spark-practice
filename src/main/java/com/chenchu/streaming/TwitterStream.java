package com.chenchu.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStream {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local[*]")
				.config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") + "/spark-wherehouse")
				.appName("spark session example").getOrCreate();
		sparkSession.sparkContext().setLogLevel("OFF");
		JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
				Durations.seconds(5));

		final String consumerKey = "*********************";
		final String consumerSecret = "********************";
		final String accessToken = "**************************";
		final String accessTokenSecret = "**********************";

		ConfigurationBuilder cb = new ConfigurationBuilder().setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
				.setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken)
				.setOAuthAccessTokenSecret(accessTokenSecret);

		OAuthAuthorization auth = new OAuthAuthorization(cb.build());

		JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc, auth, new String[] { "bigdata" });

		JavaDStream<String> statses = tweets.map(status -> status.getText());
		statses.print();
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
