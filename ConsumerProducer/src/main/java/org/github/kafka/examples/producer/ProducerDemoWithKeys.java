package org.github.kafka.examples.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.kafka.examples.common.ApplicationConstants;
import org.github.kafka.examples.common.KafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) {
        Properties properties = KafkaProperties.getKafkaProperties();
        //created a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 30; i < 33; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(ApplicationConstants.INPUT_TOPIC, "id" + i, "Message " + i);
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("offset : " + metadata.offset());
                    logger.info("topic : " + metadata.topic());
                    logger.info("partition : " + metadata.partition());
                    logger.info("timestamp : " + metadata.timestamp());
                    logger.info("serializedKeySize : " + metadata.serializedKeySize());
                    logger.info("serializedValueSize : " + metadata.serializedValueSize());
                } else {
                    logger.error("error");
                }
            });
        }
        // Flush the messages and close the producer
        producer.close();
    }
}
