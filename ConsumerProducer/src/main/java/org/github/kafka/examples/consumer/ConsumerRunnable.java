package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {
    private CountDownLatch latch;
    private KafkaConsumer kafkaConsumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public ConsumerRunnable(CountDownLatch latch, KafkaConsumer kafkaConsumer) {
        this.latch = latch;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void run() {
        try{
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : records) {
                    logger.info(record.key() + ":" + record.value());
                    logger.info(record.partition() + ":" + record.offset());
                }
            }
        } catch (WakeupException e){
            logger.error("Wakeup Exception Occurred");
        } finally {
            kafkaConsumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        kafkaConsumer.wakeup();
    }
}
