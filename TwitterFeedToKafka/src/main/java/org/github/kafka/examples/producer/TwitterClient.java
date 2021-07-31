package org.github.kafka.examples.producer;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TwitterClient {

    private final String consumerKey = "39iIflhBMONcZ5Tcz6XhWQ4G3";

    private final String consumerSecret = "1DmQrcieX3lfmNtvoQR5QeTYrtqJPbwNiByTkNzsuuXBX5vCEQ";

    private final String token = "1056689803901624320-UPXcUF3PbHmXIusPfH2DdrTSnCF58l";

    private final String tokenSecret = "MUr2EI4dOMdBkzdD33OWyRAIbIPF9QSVj6s6VkZ0QIcu0";

    public Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
         *
         * BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
         * */
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        /**
         * Optional: set up some followings and track terms
         * List<Long> followings = Lists.newArrayList(1234L, 566788L);
         * hosebirdEndpoint.followings(followings);
         *
         */
        hosebirdEndpoint.trackTerms(terms);
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        /*.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events*/
        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        return hosebirdClient;
    }
}
