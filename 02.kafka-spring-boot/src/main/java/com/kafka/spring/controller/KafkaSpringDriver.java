package com.kafka.spring.controller;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({"com.kafka.spring.service","com.kafka.spring.controller"})
public class KafkaSpringDriver {
	
	public static void main(String[] args) throws Exception{
		SpringApplication.run(KafkaSpringDriver.class,args);
	}

}
