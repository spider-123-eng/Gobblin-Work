package com.gobblin.kafka.hdfs;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
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
		int mb = 1024 * 1024;
		final String topic = AppConfiguration.KAFKA_SERVERS_TOPIC;
		String pattern = "YYYY-MM-dd:HH:mm:ss";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

		// get Runtime instance
		Runtime instance = Runtime.getRuntime();
		String json = "";

		for (int i = 1; i <= 15; i++) {

			String date = simpleDateFormat.format(new Date());
			if (i % 4 == 0) {
				long maxMem = (instance.maxMemory() / mb);
				json = "" + "{" + "\"timestamp\": \"" + date + "\"" + ", \"type\": \"Heap utilization statistics\""
						+ ",\"level\":  " + i + "," + "\"message\": \"Max Memory in MB:\"" + maxMem + "}" + "";
				sendMessage(topic, json, producer);
			} else if (i % 2 == 0) {
				long totMem = (instance.totalMemory() / mb);
				json = "" + "{" + "\"timestamp\": \"" + date + "\"" + ", \"type\": \"Heap utilization statistics\""
						+ ",\"level\":  " + i + "," + "\"message\": \"Total Memory in MB:\"" + totMem + "}" + "";
				sendMessage(topic, json, producer);
			} else if (i % 3 == 0) {
				long freeMem = (instance.freeMemory() / mb);
				json = "" + "{" + "\"timestamp\": \"" + date + "\"" + ", \"type\": \"Heap utilization statistics\""
						+ ",\"level\":  " + i + "," + "\"message\": \"Free Memory in MB:\"" + freeMem + "}" + "";
				sendMessage(topic, json, producer);
			} else if (i % 5 == 0) {
				long usedMem = ((instance.totalMemory() - instance.freeMemory()) / mb);
				json = "" + "{" + "\"timestamp\": \"" + date + "\"" + ", \"type\": \"Heap utilization statistics\""
						+ ",\"level\":  " + i + "," + "\"message\": \"Used Memory in MB:\"" + usedMem + "}" + "";

				sendMessage(topic, json, producer);

			} else {
				long usedMem = instance.availableProcessors();
				json = "" + "{" + "\"timestamp\": \"" + date + "\"" + ", \"type\": \"Heap utilization statistics\""
						+ ",\"level\":  " + i + "," + "\"message\": \"Available Processors:\"" + usedMem + "}" + "";

				sendMessage(topic, json, producer);

			}
			// add a delay of 2 seconds
			Thread.sleep(2000);

		}

		producer.close();

		System.out.println("DONE!");

	}

	public static void sendMessage(String topic, String json, KafkaProducer<byte[], byte[]> producer)
			throws InterruptedException {

		ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic,
				json.getBytes(StandardCharsets.UTF_8));
		producer.send(producerRecord);

	}
}