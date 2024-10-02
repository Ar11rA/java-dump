package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class FileStreamProducer {
    private static final String TOPIC_NAME = "file-content-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String directoryPath = "/opt/archive/"; // Specify the directory path
            Files.list(Paths.get(directoryPath)).forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    String filename = filePath.getFileName().toString();
                    StringBuilder contentBuilder = new StringBuilder();
                    try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            contentBuilder.append(line).append("\n");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    String content = contentBuilder.toString();
                    System.out.println("Sending content of length: " + content.length() + " with author " + filename);
                    // Send the filename as key and content as value
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, filename, content);
                    producer.send(record); // Send asynchronously
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
