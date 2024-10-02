package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaStreamsProducer {
    public static void main(String[] args) {
        String topicName = "sample-stream-data";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i < 10; i++) {
                String key = "key-" + i;
                String value = "value-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
                producer.send(record);
                System.out.println("Produced record: " + key + " -> " + value);
            }
        }
    }
}