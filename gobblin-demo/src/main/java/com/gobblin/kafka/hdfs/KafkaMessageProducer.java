package com.gobblin.kafka.hdfs;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Kafka producer to push data as byte[] 
 */
public class KafkaMessageProducer {
	public static void main(String[] args) throws InterruptedException {
		System.out.println("Started...");

		Properties producerProperties;
		KafkaProducer<byte[], byte[]> producer;
		producerProperties = new Properties(System.getProperties());

		producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfiguration.KAFKA_SERVERS_CONFIG);

		producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		producer = new KafkaProducer<>(producerProperties);

		final String topic = AppConfiguration.KAFKA_SERVERS_TOPIC;
		for (int i = 1; i < 100; i++) {
			final String json = "" + "{" + "\"timestamp\": \"2015-08-21T17:08:45-0400\"" + ", \"type\": \"SYSTEM\""
					+ ",\"level\":  " + i + "," + "\"message\": \"RAM usage above 90 per cent utilization\"" + " }"
					+ "";

			ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic,
					json.getBytes(StandardCharsets.UTF_8));
			producer.send(producerRecord);
			// add a delay of 1 seconds
			Thread.sleep(1000);
		}
		producer.close();

		System.out.println("DONE!");

	}
}