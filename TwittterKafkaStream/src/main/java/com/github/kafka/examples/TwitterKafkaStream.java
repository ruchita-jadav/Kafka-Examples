package com.github.kafka.examples;

import com.github.kafka.examples.constants.StreamProperties;
import com.google.gson.JsonParser;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.github.kafka.examples.constants.StreamProperties.TWITTER_TWEETS_FOLLOWER_TOPIC;

public class TwitterKafkaStream {
    static Logger logger = LoggerFactory.getLogger(TwitterKafkaStream.class);

    public static void main(String[] args) {
        Properties properties = StreamProperties.getStreamProperties();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> twitterTopicStream = streamsBuilder.stream(StreamProperties.TWITTER_TWEETS_TOPIC) ;
        KStream<String, String> filterStream = twitterTopicStream.filter((key, value) ->
                extractUserFollowerJson(value) > 10000
        );
        filterStream.to(TWITTER_TWEETS_FOLLOWER_TOPIC);
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );
        kafkaStreams.start();
    }

    private static int extractUserFollowerJson(String value) {
        try {
            return JsonParser.parseString(value)
                    .getAsJsonObject().get("user").
                            getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            logger.error("NullPointerException");
        }
        return 0;
    }
}
