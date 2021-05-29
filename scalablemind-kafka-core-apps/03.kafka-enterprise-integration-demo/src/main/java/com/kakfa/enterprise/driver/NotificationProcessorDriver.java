package com.kakfa.enterprise.driver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kakfa.enterprise.util.KafkaUtil;

public class NotificationProcessorDriver {
	private static ObjectMapper mapper = new ObjectMapper();

	private static final String GROUP_NAME = "consumer-notificationgroup";
	
	public static void main(String[] args) {
		
		Properties kafkaProperties = KafkaUtil.createConsumerProperties(GROUP_NAME);
		
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(kafkaProperties);
		
		try {
			readAndPrint(kafkaConsumer,GROUP_NAME);		
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			kafkaConsumer.close();
		}
		
	}
	
	
	public static void readAndPrint(KafkaConsumer<String, String> kafkaConsumer,String groupName) {
		List<String> topicList = Arrays.asList("customerpaymenttopic");
		
		try {
			
			kafkaConsumer.subscribe(topicList);
			
			while(true) {
				ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(10);
				
				for(ConsumerRecord<String,String> record:consumerRecords) {
					String valueJson = record.value();
					JsonNode jsonNode = mapper.readTree(valueJson);
					
					String customerId = jsonNode.get("customerId").asText();
					String email = jsonNode.get("email").asText();
					
					System.out.println("Notification sent");
					System.out.println("----------------");
					System.out.println(String.format("customerId:[%s], email:[%s]",customerId,email));
				}
				
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			kafkaConsumer.close();
		}
	}
}
