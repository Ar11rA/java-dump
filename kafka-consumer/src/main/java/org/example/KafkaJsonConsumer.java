package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaJsonConsumer {
    private static final String TOPIC_NAME = "json-topic"; // Topic to consume from
    private static final String DLQ_TOPIC_NAME = "json-dlq-topic"; // Topic to consume from
    private static final String BOOTSTRAP_SERVERS = "localhost:29092"; // Kafka broker address
    private static final String GROUP_ID = "consumer-group-json"; // Consumer group ID
    private static final int MAX_RETRIES = 3;

    public static void main(String[] args) {
        // Configure the consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start from the earliest message if no offset is present

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME)); // Subscribe to the topic

        // DLQ Producer configuration
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> dlqProducer = new KafkaProducer<>(producerProps);


        ObjectMapper objectMapper = new ObjectMapper(); // For JSON deserialization

        try {
            while (true) {
                // Poll for new records
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    String jsonString = record.value();
                    try {
                        // Deserialize JSON message
                        Message message = objectMapper.readValue(jsonString, Message.class);
                        int value = message.getValue();

                        System.out.println("Consumed message: " + jsonString + " | Partition: " + record.partition() + " | Offset: " + record.offset() + "|" + value);

                        // Process the message
                        boolean processedSuccessfully = processMessage(message);
                        if (!processedSuccessfully) {
                            // Retry logic: attempt up to MAX_RETRIES if processing fails
                            for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                                System.out.println("Retrying message... Attempt: " + attempt);
                                processedSuccessfully = processMessage(message);
                                if (processedSuccessfully) {
                                    System.out.println("Message processed successfully after retry: " + jsonString);
                                    break;
                                }
                            }

                            if (!processedSuccessfully) {
                                System.err.println("Failed to process message after " + MAX_RETRIES + " attempts: " + jsonString);
                                // Optionally, send the message to a dead-letter queue (DLQ) for further investigation
                                // For example, produce this message to a DLQ topic or log it for manual processing
                                ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(DLQ_TOPIC_NAME, record.key(), record.value());
                                RecordMetadata metadata = dlqProducer.send(dlqRecord).get(); // Synchronous send
                                System.out.println("Message sent to DLQ topic: " + metadata.topic() + " partition: " + metadata.partition() + " offset: " + metadata.offset());
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + jsonString);
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    // Simulated message processing function
    private static boolean processMessage(Message message) throws Exception {
        int value = message.getValue();
        if (value % 3 == 0) {
            return false;
        }
        // Simulate successful processing
        System.out.println("Processed message successfully: " + message.getValue());
        return true;
    }

    // Message class for JSON deserialization
    public static class Message {
        private String kind;
        private int value;

        public Message() {
        }

        public Message(String kind, int value) {
            this.kind = kind;
            this.value = value;
        }

        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }
}
