package com.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
// import java.util.Arrays;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaMessageConsumer {

    public static void main(String[] args) {
        // Kafka consumer configuration settings
        String topicName = "quickstart-events";
        Properties props = new Properties();
        
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        // props.put("security.protocol", "SASL_PLAINTEXT");
        // props.put("sasl.mechanism", "PLAIN");
        // props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username=\"{{ CLUSTER_API_KEY }}\"   password=\"{{ CLUSTER_API_SECRET }}\";");
        // props.put("ssl.endpoint.identification.algorithm","https");

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(topicName));

        // Poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}




// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.apache.kafka.clients.consumer.ConsumerRecords;
// import org.apache.kafka.clients.consumer.KafkaConsumer;
// import org.apache.kafka.common.serialization.StringDeserializer;
 
// import java.util.Arrays;
// import java.util.Properties;
 
// public class CentralConsumer {
 
//   public static void main(final String[] args) throws Exception {
//     final String topic = "<topic>";
 
//     final Properties props = new Properties();
 
//     props.put("bootstrap.servers", "<broker>");
//     props.put("group.id", "<groupID>");
//     props.put("auto.offset.reset", "latest");
//     props.put("enable.auto.commit", "false");
//     props.put("key.deserializer", StringDeserializer.class.getName());
//     props.put("value.deserializer", StringDeserializer.class.getName());
 
     
//     props.put("security.protocol", "SASL_PLAINTEXT");
//     props.put("sasl.mechanism", "PLAIN");
//     props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username=\"{{ CLUSTER_API_KEY }}\"   password=\"{{ CLUSTER_API_SECRET }}\";");
//     props.put("ssl.endpoint.identification.algorithm","https");
     
 
//     final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//     consumer.subscribe(Arrays.asList(topic));
 
//     try {
//       while (true) {
//         ConsumerRecords<String, String> records = consumer.poll(100);
//         for (ConsumerRecord<String, String> record : records) {
//           String key = record.key();
//           String value = record.value();
//           System.out.printf("Consumed record with key %s and value %s, %n", key, value);
//         }
//         consumer.commitSync();
//       }
//     } finally {
//       consumer.close();
//     }
//   }
 
// }
