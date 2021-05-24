package com.kafka.spring.service.impl;

import java.io.IOException;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.kafka.spring.service.ConsumerService;

@Component("consumerService")
public class ConsumerServiceImpl implements ConsumerService {
	
	@KafkaListener(topics = "springtopic", groupId = "spring-group")
    public void readMessage(String message) throws IOException {
        System.out.println("Message consumed >>>>>>>>>>>"+message);
    }
}
