package com.gobblin.sequence.hdfs;

import java.io.IOException;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaExtractor;
import kafka.message.MessageAndOffset;

/**
 * An implementation of {@link RawKafkaExtractor} for extracting the sequence data.
 *
 * <p>
 * This extractor is used to extract the key,value pairs from  the assigned input sequence data.
 * </p>
 *
 */
public class RawKafkaExtractor extends KafkaExtractor<String, Tuple<byte[], byte[]>> {

	public RawKafkaExtractor(WorkUnitState state) {
		super(state);
	}

	@Override
	public String getSchema() throws IOException {
		return null;
	}

	@Override
	protected Tuple<byte[], byte[]> decodeRecord(MessageAndOffset messageAndOffset) throws IOException {

		byte[] keys = getBytes(messageAndOffset.message().key());
		byte[] value = getBytes(messageAndOffset.message().payload());
		return new Tuple<byte[], byte[]>(keys, value);
	}

}
