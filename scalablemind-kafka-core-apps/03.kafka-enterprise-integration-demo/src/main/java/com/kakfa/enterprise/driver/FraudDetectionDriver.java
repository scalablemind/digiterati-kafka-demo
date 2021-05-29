package com.kakfa.enterprise.driver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kakfa.enterprise.util.KafkaUtil;

public class FraudDetectionDriver {
	
	private static ObjectMapper mapper = new ObjectMapper();

	private static final String GROUP_NAME = "consumer-frauddetectiongroup";
	
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
	
	
	private static void readAndPrint(KafkaConsumer<String, String> kafkaConsumer,String groupName) {
		List<String> topicList = Arrays.asList("frauddetectionsourcetopic");
		
		try {
			
			kafkaConsumer.subscribe(topicList);
			
			while(true) {
				ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(10);
				
				for(ConsumerRecord<String,String> record:consumerRecords) {
					String valueJson = record.value();
					JsonNode jsonNode = mapper.readTree(valueJson);
					
					String customerId = jsonNode.get("customerId").asText();
					
					String isFraud = "N";
					if(customerId.equals("200")) {
						isFraud = "Y";
					} 
					sendFraudNotification(customerId,isFraud);
					System.out.println(String.format("customerId:[%s], isFraud:[%s], remarks:[fraud status updated]",customerId,isFraud));
				}
				
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			kafkaConsumer.close();
		}
	}
	
	private static void sendFraudNotification(String customerId,String isFraud) {
		KafkaProducer<String, String> producerObj = new KafkaProducer<String, String>(KafkaUtil.createProducerProperties());
		try {
			String json = String.format("{ \"customerId\" : \"%s\", \"isFraud\" : \"%s\"}",customerId,isFraud);
			producerObj.send(new ProducerRecord<String, String>("frauddetectiontargettopic", json));
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			producerObj.close();
		}

	}
	

}
