package io.conduktor.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class D_ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(D_ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Performing Producer Demo....");

        String bootstrapServer ="27.0.0.1:9092";
        String groupId = "my-first-application";
        String topic = "demo_java";

        // Create Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        // Create consumer
        KafkaConsumer<String,String > consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe consumer to our topics
        consumer.subscribe(Arrays.asList(topic));

        // Poll for new Data
        while (true){
            log.info("Polling...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String,String> record:records){
                log.info("Key: "+ record.key()+",Value: "+record.value()+
                        ",Topic: "+record.topic()+",Partition: "+record.partition()+
                        ",Offset: "+record.offset());
            }
        }
    }
}
