package com.java.kafkaconsumer.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.security.SecurityProperties.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.java.kafkaconsumer.model.ApplicationConstant;
import com.java.kafkaconsumer.model.Vehicle;

import io.netty.handler.codec.string.StringDecoder;
import kafka.serializer.DefaultDecoder;

@Service
public class KafkaConsumer {

   
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

	@KafkaListener(group = "group_json", topics = ApplicationConstant.TOPIC_NAME, containerFactory = "userKafkaListenerFactory")
	public void receivedMessage(@Payload  Vehicle message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) throws JsonProcessingException {
		//ObjectMapper mapper = new ObjectMapper();
		//String jsonString = mapper.writeValueAsString(message);
	//	logger.info("Json message received using Kafka listener " + jsonString);
	
	
		SparkConf sparkConf = new SparkConf();
				sparkConf.setMaster("local[2]");
        sparkConf.setAppName("consumer_kafka");
        sparkConf.set("spark.cassandra.connection.host", "localhost");
			  JavaSparkContext sc = new JavaSparkContext(sparkConf);
			  JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
			  Set<String> topics = Collections.singleton("mytopic");
			  Map<String, String> kafkaParams = new HashMap<>();
			  kafkaParams.put("metadata.broker.list", "localhost:9092");
			  JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
			      String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
			  directKafkaStream.foreachRDD(rdd -> {
			    System.out.println("--- New RDD with " + rdd.partitions().size()
			        + " partitions and " + rdd.count() + " records");
			    rdd.foreach(record -> System.out.println(record._2));
			  });
			  ssc.start();
			  ssc.awaitTermination();
	}
}
