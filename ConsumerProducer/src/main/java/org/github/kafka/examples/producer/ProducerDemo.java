package org.github.kafka.examples.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.kafka.examples.common.ApplicationConstants;
import org.github.kafka.examples.common.KafkaProperties;

import java.util.Properties;

public class ProducerDemo {
//    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = KafkaProperties.getKafkaProperties();
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        //created a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //create a producer records
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(ApplicationConstants.INPUT_TOPIC, "Message 16");
        //send data
        producer.send(producerRecord);
        //Flush the message
//        producer.flush();
        // Flush the messages and close the producer
        producer.close();
    }
}
