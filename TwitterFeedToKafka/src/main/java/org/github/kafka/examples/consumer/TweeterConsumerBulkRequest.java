package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
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

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class TweeterConsumerBulkRequest {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public void conusume(RestHighLevelClient elasticsearchClient) throws IOException, InterruptedException {
        Properties properties = KafkaProperties.getConsumerProperties();
        properties.setProperty(GROUP_ID_CONFIG, "twitter-tweets-topic-g-01");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
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
            logger.info("Received " + records.count() + " records.");
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord record : records) {
                logger.info(record.key() + ":" + record.value());
                logger.info(record.partition() + ":" + record.offset());
                String message = (String) record.value();
                IndexRequest indexRequest = new IndexRequest("twitter").source(message, XContentType.JSON);
                bulkRequest.add(indexRequest);
            }
            if (bulkRequest.requests().size() > 0) {
                BulkResponse bulkResponse = elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Records are processed");
            } else {
                logger.info("No records to process");
            }
            kafkaConsumer.commitSync();
            logger.info("Commited offset");
            Thread.sleep(1000);
        }
    }
}
