package org.github.kafka.examples.consumer;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;

@SpringBootApplication
@ComponentScan({"org.github.kafka.config", "org.github.kafka.examples"})
public class ElasticSearchAPI implements CommandLineRunner {
    @Autowired
    RestHighLevelClient elasticsearchClient;

    static Logger logger = LoggerFactory.getLogger(ElasticSearchAPI.class);

    public static void main(String[] args) throws IOException {
        SpringApplication.run(ElasticSearchAPI.class, args);
    }
    /**
     * To PUT message in elastic search index.
     *
     * @param args
     * @throws Exception
     */
    /*@Override
    public void run(String... args) throws Exception {
        if (elasticsearchClient == null) {
            logger.info("elasticsearchClient is null");
        }
        String message = "{\n" +
                "    \"module\": \"elasticsearch hands on\",\n" +
                "    \"about\": \"bitcoing tweets\",\n" +
                "    \"data\": \"First bitcoin\"\n" +
                "}";
        IndexRequest indexRequest = new IndexRequest("twitter").source(message, XContentType.JSON);
        IndexResponse indexResponse = elasticsearchClient.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info("Id is :: " + id);
        elasticsearchClient.close();
    }*/

    /**
     * To Run kafka consumer.
     *
     * @param args
     * @throws Exception
     */
    @Override
    public void run(String... args) throws Exception {
//        new TweeterConsumer().conusume(elasticsearchClient);
//        new TweeterConsumerCommitSync().conusume(elasticsearchClient);
        new TweeterConsumerBulkRequest().conusume(elasticsearchClient);
    }
}

