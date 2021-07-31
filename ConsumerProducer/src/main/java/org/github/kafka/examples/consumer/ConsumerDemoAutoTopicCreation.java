package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.github.kafka.examples.common.ApplicationConstants;
import org.github.kafka.examples.common.KafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * This consumer method will create a topic which does not exist in cluster yet.
 * when broker has property "auto.create.topics.enable=true" then it will allow to create topics on any request.
 * This is the default settings, so to disable it put "auto.create.topics.enable=false" in broker configuration file.
 * and it should throw : "Error while fetching metadata with correlation id 140 : {input-topic-auto-enable-test=UNKNOWN_TOPIC_OR_PARTITION}"
 *
 */
public class ConsumerDemoAutoTopicCreation {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAutoTopicCreation.class);

    public static void main(String[] args) {
        Properties properties = KafkaProperties.getConsumerProperties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "input-topic-auto-enable-test-g1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("input-topic-auto-enable-test"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                logger.info(record.key() + ":" + record.value());
                logger.info(record.partition() + ":" + record.offset());
            }
        }
    }
}
