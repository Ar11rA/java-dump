package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

public class KafkaJsonProducer {
    private static final String TOPIC_NAME = "json-topic"; // topic name
    private static final String BOOTSTRAP_SERVERS = "localhost:29092"; // Kafka broker address
    private static final int MAX_RETRIES = 3;

    public static void main(String[] args) {
        // Configure the producer
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ObjectMapper objectMapper = new ObjectMapper(); // For JSON serialization
            Random random = new Random();
            for (int i = 0; i < 10; i++) {
                int value = random.nextInt(10) + 1; // Random value between 1 and 10
                int multiplier = random.nextInt(3) + 1; // Random multiplier between 1 and 3
                int result = value * multiplier;

                // Create JSON message
                Message message = new Message("money", value);

                boolean processedSuccessfully = false;
                int attempt = 0;

                while (attempt < MAX_RETRIES && !processedSuccessfully) {
                    try {
                        if (result % 5 == 0) {
                            throw new IllegalArgumentException("Result is divisible by 5. Simulating error.");
                        }

                        // Send the message
                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, objectMapper.writeValueAsString(message));
                        RecordMetadata metadata = producer.send(record).get(); // synchronous send

                        System.out.println("Message sent to topic: " + metadata.topic() +
                                ", partition: " + metadata.partition() +
                                ", offset: " + metadata.offset() + "\tvalue: " + objectMapper.writeValueAsString(message));

                        processedSuccessfully = true; // Mark as processed successfully
                    } catch (IllegalArgumentException e) {
                        attempt++;

                        System.err.println("Error processing message: " + objectMapper.writeValueAsString(message) + " | Attempt: " + attempt);
                        // Update the message value for next retry
                        message.setValue(random.nextInt(10) + 1);
                        value = message.getValue(); // Update the base value for new calculation
                        multiplier = random.nextInt(3) + 1; // New multiplier
                        result = value * multiplier; // Recalculate result
                        System.err.println("New processing entry: " + objectMapper.writeValueAsString(message) + " | Attempt: " + attempt);
                    } catch (Exception e) {
                        System.err.println("Unexpected error processing message: " + objectMapper.writeValueAsString(message));
                        break; // Exit the retry loop on unexpected error
                    }
                }

                if (!processedSuccessfully) {
                    System.err.println("Failed to process message after " + MAX_RETRIES + " attempts: " + objectMapper.writeValueAsString(message));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Message class for JSON serialization
    public static class Message {
        private String kind;
        private int value;

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
