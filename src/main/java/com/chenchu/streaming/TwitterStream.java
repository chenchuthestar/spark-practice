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
		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("spark session example")
				.getOrCreate();
		sparkSession.sparkContext().setLogLevel("OFF");
		JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
				Durations.seconds(5));

		final String consumerKey = "jnc5okyQhTV9Ff3oRqKvY0K5x";
		final String consumerSecret = "Qsrwwj1AngqdzJfqn5mEKCBuYQXsO6G9bjQKVJGD1bGQ7PFY7O";
		final String accessToken = "1006408317307052032-e5LgWxeYODQ03ZTLrMuSaZb90WXx8C";
		final String accessTokenSecret = "F6Dk6RYDOxFU44P6SS9qnJYNbf9VxNGiARwJFfkuXbDXs";

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
