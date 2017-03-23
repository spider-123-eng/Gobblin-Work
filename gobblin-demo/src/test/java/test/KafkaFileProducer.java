package test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class KafkaFileProducer extends Thread {

	private static final String topicName = "test";
	public static final String fileName = "/opt/gobblin/simplejson.json";

	private final KafkaProducer<byte[], byte[]> producer;

	public KafkaFileProducer(String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("client.id", "DemoProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		producer = new KafkaProducer<byte[], byte[]>(props);
	}

	public void sendMessage(String key, String value) {
		try {
			producer.send(new ProducerRecord<byte[], byte[]>(topicName, key.getBytes(StandardCharsets.UTF_8),
					value.getBytes(StandardCharsets.UTF_8))).get();
			System.out.println("Sent message: (" + key + ", " + value + ")");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		KafkaFileProducer producer = new KafkaFileProducer(topicName);
		int lineCount = 0;
		FileInputStream fis;
		BufferedReader br = null;
		try {
			fis = new FileInputStream(fileName);
			// Construct BufferedReader from InputStreamReader
			br = new BufferedReader(new InputStreamReader(fis));

			String line = null;
			while ((line = br.readLine()) != null) {
				lineCount++;
				producer.sendMessage(lineCount + "", line);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}
