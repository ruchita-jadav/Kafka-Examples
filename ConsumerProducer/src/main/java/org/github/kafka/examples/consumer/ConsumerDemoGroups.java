package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.github.kafka.examples.common.KafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class ConsumerDemoGroups {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

    public static void main(String[] args) {
        Properties properties = KafkaProperties.getConsumerProperties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "input-g1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // Regular experssion
//        kafkaConsumer.subscribe(Pattern.compile("new-topic.*"));

        // Subscribe more than one topics
//        kafkaConsumer.subscribe(Arrays.asList(ApplicationConstants.INPUT_TOPIC, ApplicationConstants.NEW_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                logger.info(record.topic());
                logger.info("key : value -> " + record.key() + ":" + record.value());
                logger.info("partition : offset -> " + record.partition() + ":" + record.offset());
            }
        }
    }
}
