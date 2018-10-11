package com.github.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Chetan Raj
 * @implNote
 * @since : 29/09/18
 */
public class TwitterProducer {

    private String consumerKey = "CgCCXb9bLgLrsbHFyTWQI8Lsdfawz";
    private String consumerSecret = "3sEifeUqk1NzadfagUlimXwUmBpzxPU84L3qDqr7PyjM4xpxHfcerw";
    private String token = "1242897014-6rongDWz6adaZHkBexXy12HOXxY1udNIWAMVI9bAtQ ";
    private String secret = "zCeOnjAR98hRSadfaAyav17DTWMKXlGxCdY606RXAs2vGoY0V";

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) {
        new TwitterProducer().runWithCustomJSONData();
    }

    public TwitterProducer() {
    }

    public void runWithCustomJSONData() {
        BlockingQueue<String> msgQueue = null;

        msgQueue = getCustomData();
        // Create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("Stopping application...");
            logger.info("Closing producer...");
            producer.close();
            logger.info("DONE!");
        }));

        // loop to send tweets to Kafka
        // on a different thread, or multiple different threads....
        String msg = null;
        try {
            //for (msg:msgQueue.poll(5, TimeUnit.SECONDS)) {
            while ((msg = msgQueue.poll(5, TimeUnit.SECONDS)) != null) {
                if (msg != null) {
                    logger.info(msg);
                    producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                logger.error("Something bad happened", e);
                            }
                        }
                    });
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("End of the application!");
    }

    public void runForTwitterData() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // Create a twitter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // Create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // loop to send tweets to Kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
            }
        }
        logger.info("End of the application!");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        // List<String> terms = Lists.newArrayList("twitter", "api");
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        //Creating a client:

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;
        // Attempts to establish a connection.
        // hosebirdClient.connect();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootStrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // Kafka 2.0 >=1.1 so we can keep this as 5. User 1 otherwise.

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // 20 ms
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString((32 * 1024))); // 32 KB batch size

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

    public BlockingQueue<String> getCustomData() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        for (int i = 0; i < 1000; i++) {
            try {
                msgQueue.put("{\"ver\":\"1.0\",\"requestType\":\"Twitter\",\"id\": \"" + generateUniqueId() + " \",\"languageId\":\"1\",\"sessionId\": \" " + i + " \",\"channel\":\" localhost\"}");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return msgQueue;
    }

    public static String generateUniqueId() {
        final DateFormat sdf = new SimpleDateFormat("DDDMMyyyyHHmmssSSS");
        return new StringBuffer().append(sdf.format(new Date(System.currentTimeMillis())))
                .append(new Random().nextInt(10)).toString();
    }

}
