package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class ExactlyOnceProducer {

    public static void main(String[] args) {
        String topicName = "exactly-once-topic"; // Topic name
        String bootstrapServers = "localhost:29092"; // Kafka broker address

        // Configure the producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Enable idempotent producer
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Enable transactions by setting a transactional ID
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-producer");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Initialize the transaction
        producer.initTransactions();

        try {
            // Start the transaction
            producer.beginTransaction();

            // Create the message to send
            String key = "test-key";
            String value = "This is an exactly-once message";

            // Send the message
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            RecordMetadata metadata = producer.send(record).get();

            System.out.println("Message sent to topic: " + metadata.topic() +
                    ", partition: " + metadata.partition() +
                    ", offset: " + metadata.offset());

            // Commit the transaction
            producer.commitTransaction();
            System.out.println("Transaction committed successfully.");
        } catch (Exception e) {
            // If something goes wrong, abort the transaction
            producer.abortTransaction();
            e.printStackTrace();
            System.out.println("Transaction aborted.");
        } finally {
            producer.close();
        }
    }
}
