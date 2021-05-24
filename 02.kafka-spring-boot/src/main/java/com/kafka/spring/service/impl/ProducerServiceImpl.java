package com.kafka.spring.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.kafka.spring.service.ProducerService;

@Component("producerService")
public class ProducerServiceImpl implements ProducerService {

	@Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {
        this.kafkaTemplate.send("springtopic", message);
    }
}
