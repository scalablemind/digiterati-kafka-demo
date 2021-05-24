package com.kafka.driver;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.OffsetKey;

public class SimpleConsumerDriver2 {
	
	
	private static Properties createProducerProperties(String groupName) {
		Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", groupName);
       // properties.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
	}
	
	public static void main(String[] args) {
		
		String groupName = "cg2";
		KafkaConsumer<String, String> kafkaConsumer1 = new KafkaConsumer<String, String>(createProducerProperties(groupName));
		
		readAndPrint(kafkaConsumer1,groupName);
	}
	
	public static void readAndPrint(KafkaConsumer<String, String> kafkaConsumer,String groupName) {
		List<String> topicList = Arrays.asList("__consumer_offsets");
		
		try {
			
			kafkaConsumer.subscribe(topicList);
			
			while(true) {
				ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(10);
				
				
				for(ConsumerRecord<String,String> consumerRecord:consumerRecords) {
					String keyStr =  new String(consumerRecord.key());
					byte[] key = keyStr.getBytes();
					if (key != null) {
						ByteBuffer keyBuffer = ByteBuffer.wrap(key);
					    Object o = GroupMetadataManager.readMessageKey(keyBuffer);
					    if (o != null && o instanceof OffsetKey) {
					        OffsetKey offsetKey = (OffsetKey) o;
					        Object groupTopicPartition = offsetKey.key();
					        String valueStr = consumerRecord.value();
					        byte[] value = valueStr.getBytes();
					        ByteBuffer buffer = ByteBuffer.wrap(value);
					       // String formattedValue = String.valueOf(GroupMetadataManager.readOffsetMessageValue(buffer));
					       // TODO: Print, store, or compute results with the new key and value 
					        System.out.println("*************************");
					        System.out.println(groupTopicPartition);
					        System.out.println(valueStr);
					    }
					}
				}
				
				System.out.println(consumerRecords.count());
				
				Thread.sleep(10000);
				
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			kafkaConsumer.close();
		}
	}

}
