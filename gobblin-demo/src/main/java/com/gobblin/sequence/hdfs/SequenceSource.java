package com.gobblin.sequence.hdfs;

import java.io.IOException;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSource;

/**
 * An implementation of {@link Source} for pulling the sequence data from kafka.
 *
 * <p>
 * This source creates a {@link gobblin.source.workunit.WorkUnit}, and uses
 * {@link RawKafkaExtractor} to pull the data from Kafka system.
 * </p>
 *
 */
public class SequenceSource<K> extends KafkaSource<K, Tuple<byte[], byte[]>> {

	@SuppressWarnings("unchecked")
	@Override
	public Extractor<K, Tuple<byte[], byte[]>> getExtractor(WorkUnitState arg0) throws IOException {
		return (Extractor<K, Tuple<byte[], byte[]>>) new RawKafkaExtractor(arg0);
	}

}
