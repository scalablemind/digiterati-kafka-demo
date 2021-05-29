package com.kafka.streaming.driver;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class StreamProcessingDriver {
	
	public static void main(String[] args) {
		
		Properties kafkaProperties = createStreamsConfig();
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<Integer, String> kstream = builder.stream("streamingtopic");
		kstream.foreach((x,y) -> System.out.println(x+"==="+y));
		
		
		Topology streamTopology = builder.build();
		
		KafkaStreams streams = new KafkaStreams(streamTopology,kafkaProperties);
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
	
	private static Properties createStreamsConfig() {
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamsprocessor");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
	}
	
}
