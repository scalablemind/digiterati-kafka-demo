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

public class PaymentProcessorDriver {
	
	private static Map<String,String> customerPaymentMapping = new HashMap<String,String>();

	private static Double totalPayment = new Double(0);
	
	private static ObjectMapper mapper = new ObjectMapper();

	private static final String GROUP_NAME = "consumer-paymentgroup";
	
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
		List<String> topicList = Arrays.asList("customerpaymenttopic");
		
		try {
			
			kafkaConsumer.subscribe(topicList);
			
			while(true) {
				ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(10);
				
				for(ConsumerRecord<String,String> record:consumerRecords) {
					String valueJson = record.value();
					JsonNode jsonNode = mapper.readTree(valueJson);
					
					String customerId = jsonNode.get("customerId").asText();
					String amount = jsonNode.get("paymentAmount").asText();
					
					customerPaymentMapping.put(customerId, amount);
					
					totalPayment += Double.valueOf(amount);

					System.out.println("Payment received");
					System.out.println("----------------");
					System.out.println(customerPaymentMapping);
					System.out.println("Total payment:"+totalPayment);
				}
				
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			kafkaConsumer.close();
		}
	}

}
