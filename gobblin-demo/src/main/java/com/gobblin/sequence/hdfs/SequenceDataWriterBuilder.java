package com.gobblin.sequence.hdfs;

import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import gobblin.writer.DataWriter;
import gobblin.writer.FsDataWriterBuilder;
import org.apache.avro.Schema;

/**
 * A {@link DataWriterBuilder} for building {@link DataWriter} that writes in
 * sequence format.
 *
 * @author revanthreddy
 */
public class SequenceDataWriterBuilder extends FsDataWriterBuilder<Schema, Tuple<byte[], byte[]>> {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public DataWriter<Tuple<byte[], byte[]>> build() throws IOException {
		Preconditions.checkNotNull(this.destination);
		Preconditions.checkArgument(!Strings.isNullOrEmpty(this.writerId));

		switch (this.destination.getType()) {
		case HDFS:
			return new SequenceDataWriter(this, this.destination.getProperties());
		default:
			throw new RuntimeException("Unknown destination type: " + this.destination.getType());
		}
	}

}
