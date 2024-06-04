package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
// import java.util.Arrays;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaMessageConsumer {

    private static final String TOPIC_NAME = "zoro";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "roro_zoro";
    private static final String KEY = "monkey_d_luffy";
    
    public static void main(String[] args) {
        // Kafka consumer configuration settings
        Properties props = new Properties();
        
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        
        // if we have the sasl security enabled. we can use this
        // props.put("security.protocol", "SASL_PLAINTEXT");
        // props.put("sasl.mechanism", "PLAIN");
        // props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username=\"{{ CLUSTER_API_KEY }}\"   password=\"{{ CLUSTER_API_SECRET }}\";");
        // props.put("ssl.endpoint.identification.algorithm","https");

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        // Poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumer received record: " + record);
                    String key = record.key();
                    String value = record.value();

                    // Filter by key
                    if (KEY.equals(key)) {
                        System.out.println("Consumer received message with key1: " + value);
                    }
                }
                consumer.commitSync();
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
