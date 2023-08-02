package io.conduktor.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class B_ProducerDemoCallback {

    private static final Logger log = LoggerFactory.getLogger(B_ProducerDemoCallback.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Performing Producer Demo....");

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){
            // Create Producer Record
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","Hello World");

            // Send the data Asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null){
                        log.info("Received new metadata : \n "+
                                "Topic: "+ metadata.topic()+ "\n"+
                                "Partition: "+metadata.partition()+ "\n"+
                                "Offset: "+metadata.offset()+ "\n"+
                                "Timestamp: "+metadata.timestamp()
                        );
                    }else {
                        log.error("Error while producing: "+ exception);
                    }
                }
            });

            try{
                Thread.sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }


        // Flush data synchronous
        producer.flush();

        // flush and close
        producer.close();
    }
}
