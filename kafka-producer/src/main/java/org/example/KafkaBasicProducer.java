package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaBasicProducer {
    public static void main(String[] args) {
        String topicName = "num-data-3"; // topic name
        String bootstrapServers = "localhost:29092"; // Kafka broker address

        // Configure the producer
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 10; i++) {
                String value = "message" + i;
                String key = String.valueOf((i % 3));
                // Create and send the message
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
                RecordMetadata metadata = producer.send(record).get(); // synchronous send

                System.out.println("Message sent to topic: " + metadata.topic() +
                        ", partition: " + metadata.partition() +
                        ", offset: " + metadata.offset() + "\tvalue: " + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
