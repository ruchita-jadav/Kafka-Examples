package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.github.kafka.examples.common.ApplicationConstants;
import org.github.kafka.examples.common.KafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TweeterConsumer {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public void conusume(RestHighLevelClient elasticsearchClient) throws IOException {
        Properties properties = KafkaProperties.getConsumerProperties();
        properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "twitter-tweets-topic-g-01");
        properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(ApplicationConstants.TWITTER_TWEETS_TOPIC));
        if (elasticsearchClient == null) {
            logger.info("elasticsearchClient is null");
            return;
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Inside shutdonw hook");
            try {
                elasticsearchClient.close();
                kafkaConsumer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                logger.info(record.key() + ":" + record.value());
                logger.info(record.partition() + ":" + record.offset());
                String message = (String) record.value();
                IndexRequest indexRequest = new IndexRequest("twitter").source(message, XContentType.JSON);
                IndexResponse indexResponse = elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info("Id is :: " + id);
            }
        }
    }
}
