package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ExactlyOnceConsumer {
    public static void main(String[] args) {
        String topicName = "exactly-once-topic"; // Topic name
        String bootstrapServers = "localhost:29092"; // Kafka broker address
        String groupId = "exactly-once-group"; // Consumer group ID

        // Configure the consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // Ensure the consumer only reads committed messages
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Create the Kafka consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList(topicName));

            System.out.println("Waiting for messages...");

            // Consume messages
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumed message with key: " + record.key() +
                            ", value: " + record.value() +
                            ", partition: " + record.partition() +
                            ", offset: " + record.offset());

                    // Simulate message processing
                    try {
                        // Process the message
                        System.out.println("Processing message...");
                        Thread.sleep(1000); // Simulate processing time
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
