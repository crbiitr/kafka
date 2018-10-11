package com.github.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 03/10/18

    Data to insert in Elastic search
        URL: /twitter/tweets/1

        {
        "ver": "1.0",
        "requestType": "Twitter",
        "id": "Test123",
        "languageId": "1",
        "sessionId": "Session123",
        "channel": " localhost"
        }

        /twitter/tweets/cKclYWYBWgTfUxasnGeg

        /twitter/tweets/b6ciYWYBWgTfUxasNWeW
 */
public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException {
        final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
        String topics = "twitter_tweets";

        RestHighLevelClient client = createClient();
        String tweet = "{\"ver\":\"1.0\",\"requestType\":\"Twitter\",\"id\": \"" + 0000000002 + " \",\"languageId\":\"1\",\"sessionId\": \" " + 00000002 + " \",\"channel\":\" localhost_test\"}";

        KafkaConsumer<String, String> consumer = createConsumer(topics);

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0
            logger.info("Received: ", records.count());
            for (ConsumerRecord<String, String> record: records) {
                logger.info("Keys: " + record.key() + " Value: " + record.value() + "\n");
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset() + "\n");


                IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(record.value(), XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info("ElasticSearch Document ID: {}", id);

                try {
                    logger.info("Thread is sleeping ======> ");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("Committing offsets...");
            consumer.commitSync();
            logger.info("Offsets have been committed");
        }


        // Close the client gracefully
        //client.close();  // Commenting just because some testing
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootStrapServers = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        /*create Producer properties*/
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //Disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); // take only 10 records at a time

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        //consumer.subscribe(Collections.singleton("first_topic")); //If you want only one topic to consume
        consumer.subscribe(Arrays.asList(topic)); //If you want to consume multiple topics

        return consumer;
    }

    public static RestHighLevelClient createClient() {

        // Replace with new credential, because i regenerated new userName and password
        String hostname = "kafka-elastic-search-1824318869.ap-southeast-2.bonsaisearch.net";
        String userName = "4pkybisg1u";
        String password = "xe5zky018s";

        // don't do if you run a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
