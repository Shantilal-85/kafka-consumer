package com.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerExample {
    private static final String TOPIC_NAME = "zoro";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            // producer.send(new ProducerRecord<>(TOPIC_NAME, "Value2 from Producer 1"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "monkey_d_luffy", "Value1 from Producer 1"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "monkey_d_luffy", "Value2 from Producer 1"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "monkey_d_luffy", "Value3 from Producer 1"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "vinsmoke_sanji", "Value1 from Producer 2"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "vinsmoke_sanji", "Value2 from Producer 2"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "vinsmoke_sanji", "Value3 from Producer 2"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "vinsmoke_sanji", "Value4 from Producer 2"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
