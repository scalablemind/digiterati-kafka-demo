package com.kafka.spring.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.spring.service.ProducerService;

@RestController
@RequestMapping("/springkafka")
public class KafkaSpringController {
	
	@Autowired
	@Qualifier("producerService")
	private ProducerService producer;


    @GetMapping(value = "/sendMessage/{message}")	
    public void sendMessageToKafkaTopic(@PathVariable("message") String message) {
        this.producer.sendMessage(message);
    }

}
