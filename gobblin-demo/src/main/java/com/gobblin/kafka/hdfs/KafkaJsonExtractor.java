package com.gobblin.kafka.hdfs;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaExtractor;

import java.io.IOException;

import kafka.message.MessageAndOffset;

/**
 *Extractor to extract the json data from kafka .
 */
public class KafkaJsonExtractor extends KafkaExtractor<String, byte[]> {

	public KafkaJsonExtractor(WorkUnitState state) {
		super(state);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getSchema() throws IOException {
		return this.topicName;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected byte[] decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
		return getBytes(messageAndOffset.message().payload());
	}

}