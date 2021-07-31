package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.github.kafka.examples.common.ApplicationConstants;
import org.github.kafka.examples.common.KafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(latch,getKafkaConsumer());
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            logger.info("Inside shutdonw hook");
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Interrupt exception occurred");
        } finally {
            logger.info("Application is closing");
        }
    }

    private static KafkaConsumer getKafkaConsumer() {
        Properties properties = KafkaProperties.getConsumerProperties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "input-g1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Arrays.asList(ApplicationConstants.INPUT_TOPIC));
            return kafkaConsumer;
        }
    }
}
