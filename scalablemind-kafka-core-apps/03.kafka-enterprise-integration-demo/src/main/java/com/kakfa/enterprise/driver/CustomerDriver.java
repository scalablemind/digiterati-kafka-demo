package com.kakfa.enterprise.driver;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kakfa.enterprise.util.KafkaUtil;

public class CustomerDriver {
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		
		try {
			while(true) {
				System.out.println("Enter customer ID:");
				String customerId = scanner.next();
				
				System.out.println("Enter payment amount:");
				String paymentAmount = scanner.next();
				
				System.out.println("Enter email:");
				String email = scanner.next();
				
				
				String json = String.format("{ \"customerId\" : \"%s\", \"paymentAmount\" : \"%s\", \"email\" : \"%s\" }",customerId,paymentAmount,email);
				
				
				/*KafkaProducer<String, String> fraudProducerObj = new KafkaProducer<String, String>(KafkaUtil.createProducerProperties());
				try {
					fraudProducerObj.send(new ProducerRecord<String, String>("frauddetectionsourcetopic", json));
				} catch(Exception ex) {
					ex.printStackTrace();
				} finally {
					fraudProducerObj.close();
				}
				
				if(isFraud(customerId)) {
					System.out.println("Detected fraud. Not sending event for the customerId:"+customerId);
					continue;
				}*/
				
				KafkaProducer<String, String> producerObj = new KafkaProducer<String, String>(KafkaUtil.createProducerProperties());
				try {
					producerObj.send(new ProducerRecord<String, String>("customerpaymenttopic", json));
				} catch(Exception ex) {
					ex.printStackTrace();
				} finally {
					producerObj.close();
				}
				System.out.println("payment updated and event broadcasted");
			}
		} finally {
			scanner.close();
		}
	}
	
	
	
	private static boolean isFraud(String customerId) {
		
		Properties kafkaProperties = KafkaUtil.createConsumerProperties("consumer-paymentfrauddetection");
		
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(kafkaProperties);
		
		List<String> topicList = Arrays.asList("frauddetectiontargettopic");
		
		boolean isFraud = false;
		
		try {
			
			kafkaConsumer.subscribe(topicList);
			
			boolean closeConsumer = false;
			while(true) {
				ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(10);
				
				if(closeConsumer) {
					break;
				}
				
				for(ConsumerRecord<String,String> record:consumerRecords) {
					String valueJson = record.value();
					JsonNode jsonNode = mapper.readTree(valueJson);
					
					String jsonCustomerId = jsonNode.get("customerId").asText();
					String jsonIsFraud = jsonNode.get("isFraud").asText();
					
					if(jsonCustomerId.equals(customerId)) {
						isFraud = jsonIsFraud.equals("Y");
						closeConsumer = true;
						break;
					}
				}
				
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			kafkaConsumer.close();
		}
		
		return isFraud;
	}
	
	
	
	

}
