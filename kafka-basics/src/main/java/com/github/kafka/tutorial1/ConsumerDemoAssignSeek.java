package com.github.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by B0204104 on 16/09/18.
 * assign and seek are mostly used to replay data or fetch a specific message
 */
public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootStrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        String topics = "first_topic";
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay data or fetch a specific message

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info("Keys: " + record.key() + " Value: " + record.value() + "\n");
                logger.info("Partition: " + record.partition() + " Offset: " + record.offset() + "\n");

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break; // to exit the for loop
                }
            }
        }
        logger.info("Exiting the application");
    }
}
