package com.github.kafka.examples.constants;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamProperties {
    public static final String TWITTER_TWEETS_TOPIC = "twitter-tweets-topic";
    public static final String TWITTER_TWEETS_FOLLOWER_TOPIC = "twitter-tweets-followers-topic";


    public static java.util.Properties getStreamProperties() {
        Properties properties = new java.util.Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example"); // This is consumer group id
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return properties;
    }
}
