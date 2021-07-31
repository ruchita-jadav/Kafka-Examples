package org.github.kafka.examples.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.kafka.examples.common.ApplicationConstants;
import org.github.kafka.examples.common.KafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.jvm.hotspot.code.PCDesc;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducerSafe {

    private static Logger logger = LoggerFactory.getLogger(TwitterProducerSafe.class);

    public static void main(String[] args) {
        // Create twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        List<String> terms = Lists.newArrayList("covid","hospital","government","bitcoin");
        Client hosebirdClient = new TwitterClient().createTwitterClient(msgQueue,terms);
        hosebirdClient.connect();

        // create a kafka producer
        Properties properties = KafkaProperties.getKafkaProperties();
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // High throughput producer ( at the expense of bit latency and CPU)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //loop to send tweets to kafka
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error(e.toString());
                hosebirdClient.stop();
            }
            if (msg != null) {
                logger.info("Twitter Message : " + msg);
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(ApplicationConstants.TWITTER_TWEETS_TOPIC, msg);
                producer.send(producerRecord, ((metadata, exception) -> {
                    if (exception == null) {
                        logger.info(metadata.topic() + " :: " + metadata.partition() + " :: " + metadata.offset());
                    } else {
                        logger.error("Producer can't send message - " + exception.getMessage());
                    }
                }));
            }
        }

        //Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("closing application ");
                    hosebirdClient.stop();
                    producer.close();
                    logger.info("Done !!! ");
                }
        ));
    }
}
