package com.kafka.driver;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumerDriverr {
	
	
	private static Properties createProducerProperties() {
		Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "consumer-group");
        return properties;
	}
	
	public static void main(String[] args) {
		
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(createProducerProperties());
		
		List<String> topicList = Arrays.asList("simpleproducerdrivertopic");
		
		try {
			kafkaConsumer.subscribe(topicList);
			
			while(true) {
				ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(10);
				
				for(ConsumerRecord<String,String> record:consumerRecords) {
					System.out.println("-------------start-----------------");
					System.out.println(record.key());
					System.out.println(record.value());
					System.out.println("--------------end----------------");
				}
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			kafkaConsumer.close();
		}
 		
	}

}
