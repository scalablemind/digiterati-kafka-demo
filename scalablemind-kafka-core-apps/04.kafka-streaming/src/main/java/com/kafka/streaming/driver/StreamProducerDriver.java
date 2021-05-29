package com.kafka.streaming.driver;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class StreamProducerDriver {
	
	private static AtomicInteger incrementer = new AtomicInteger();
	
	
	public static void main(String[] args) {
		
		Properties kafkaProperties = createProducerProperties();
		int number = incrementer.addAndGet(1);
		
		KafkaProducer<String, String> producerObj = new KafkaProducer<String, String>(kafkaProperties);
		try {
			producerObj.send(new ProducerRecord<String, String>("streamingtopic", "Knowledge should be free"+number));
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			producerObj.close();
		}
		
	}
	
	private static Properties createProducerProperties() {
		Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
	}

}
