package com.kafka.driver;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumerDriverr {
	
	
	private static Properties createProducerProperties(String groupName) {
		Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", groupName);
        properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
	}
	
	public static void main(String[] args) {
		
		String groupName = "cg1";
		KafkaConsumer<String, String> kafkaConsumer1 = new KafkaConsumer<String, String>(createProducerProperties(groupName));
		
		readAndPrint(kafkaConsumer1,groupName);
	}
	
	public static void readAndPrint(KafkaConsumer<String, String> kafkaConsumer,String groupName) {
		List<String> topicList = Arrays.asList("simpledrivertopic");
		
		try {
			
			kafkaConsumer.subscribe(topicList);
			
			while(true) {
				ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(10);
				
				for(ConsumerRecord<String,String> record:consumerRecords) {
					System.out.println(groupName);
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
