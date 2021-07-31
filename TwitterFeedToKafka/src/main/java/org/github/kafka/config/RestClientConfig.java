package org.github.kafka.config;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.config.AbstractElasticsearchConfiguration;

@Configuration
public class RestClientConfig extends AbstractElasticsearchConfiguration {

    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {
        String hostname = "cluster-1-4943818538.ap-southeast-2.bonsaisearch.net:443";
        String username = "6yn2iytbqf";
        String password = "zlb2hx8jl0";
        final ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .connectedTo(hostname)
                .usingSsl()
                .withBasicAuth(username, password)
                .build();
        return RestClients.create(clientConfiguration).rest();
    }
}
