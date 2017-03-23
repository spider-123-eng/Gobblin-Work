package com.gobblin.sequence.hdfs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.FinalState;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.JobConfigurationUtils;
import gobblin.util.WriterUtils;
import gobblin.util.recordcount.IngestionRecordCountProvider;
import gobblin.writer.DataWriter;
import gobblin.writer.FsDataWriterBuilder;

/**
 * An implementation of {@link DataWriter} does the work of setting the
 * output/staging dir and creating the FileSystem instance.
 *
 */
public class SequenceDataWriter<D> implements DataWriter<D>, FinalState {
	private static final Logger LOG = LoggerFactory.getLogger(SequenceDataWriter.class);

	public static final String WRITER_INCLUDE_RECORD_COUNT_IN_FILE_NAMES = ConfigurationKeys.WRITER_PREFIX
			+ ".include.record.count.in.file.names";

	protected final State properties;
	protected final String id;
	protected final int numBranches;
	protected final int branchId;
	protected final String fileName;
	protected final FileSystem fs;
	protected final Path stagingFile;
	protected Path outputFile;
	protected final String allOutputFilesPropName;
	protected final boolean shouldIncludeRecordCountInFileName;
	protected final int bufferSize;
	protected final short replicationFactor;
	protected final long blockSize;
	protected final FsPermission filePermission;
	protected final FsPermission dirPermission;
	protected final Optional<String> group;
	protected final Option optCom = SequenceFile.Writer.compression(CompressionType.NONE);
	protected final Schema schema;
	Writer writer = null;
	protected final AtomicLong count = new AtomicLong(0);

	public SequenceDataWriter(FsDataWriterBuilder<Schema, Tuple<byte[], byte[]>> builder, State properties)
			throws IOException {

		this.properties = properties;
		this.id = builder.getWriterId();
		this.numBranches = builder.getBranches();
		this.branchId = builder.getBranch();
		this.fileName = builder.getFileName(properties);

		Configuration conf = new Configuration();
		// Add all job configuration properties so they are picked up by Hadoop
		JobConfigurationUtils.putStateIntoConfiguration(properties, conf);

		this.fs = WriterUtils.getWriterFS(properties, this.numBranches, this.branchId);
		conf.set("fs.default.name", fs.getUri().toString());
		// Initialize staging/output directory
		this.stagingFile = new Path(WriterUtils.getWriterStagingDir(properties, this.numBranches, this.branchId),
				this.fileName + ".seq");
		this.outputFile = new Path(WriterUtils.getWriterOutputDir(properties, this.numBranches, this.branchId),
				this.fileName + ".seq");
		this.allOutputFilesPropName = ForkOperatorUtils.getPropertyNameForBranch(
				ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS, this.numBranches, this.branchId);

		// Deleting the staging file if it already exists, which can happen if
		// the
		// task failed and the staging file didn't get cleaned up for some
		// reason.
		// Deleting the staging file prevents the task retry from being blocked.
		if (this.fs.exists(this.stagingFile)) {
			LOG.warn(String.format("Task staging file %s already exists, deleting it", this.stagingFile));
			HadoopUtils.deletePath(this.fs, this.stagingFile, false);
		}

		this.shouldIncludeRecordCountInFileName = properties.getPropAsBoolean(ForkOperatorUtils
				.getPropertyNameForBranch(WRITER_INCLUDE_RECORD_COUNT_IN_FILE_NAMES, this.numBranches, this.branchId),
				false);

		this.bufferSize = properties.getPropAsInt(ForkOperatorUtils
				.getPropertyNameForBranch(ConfigurationKeys.WRITER_BUFFER_SIZE, this.numBranches, this.branchId),
				ConfigurationKeys.DEFAULT_BUFFER_SIZE);

		this.replicationFactor = properties.getPropAsShort(
				ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_REPLICATION_FACTOR,
						this.numBranches, this.branchId),
				this.fs.getDefaultReplication(this.outputFile));

		this.blockSize = properties.getPropAsLong(ForkOperatorUtils
				.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_BLOCK_SIZE, this.numBranches, this.branchId),
				this.fs.getDefaultBlockSize(this.outputFile));

		this.filePermission = HadoopUtils.deserializeWriterFilePermissions(properties, this.numBranches, this.branchId);

		this.dirPermission = HadoopUtils.deserializeWriterDirPermissions(properties, this.numBranches, this.branchId);

		this.group = Optional.fromNullable(properties.getProp(ForkOperatorUtils
				.getPropertyNameForBranch(ConfigurationKeys.WRITER_GROUP_NAME, this.numBranches, this.branchId)));

		// Create the parent directory of the output file if it does not exist
		WriterUtils.mkdirsWithRecursivePermission(this.fs, this.outputFile.getParent(), this.dirPermission);

		this.schema = builder.getSchema();
		Path outputPath = new Path(this.stagingFile.toString());
		Option optKey = SequenceFile.Writer.keyClass(BytesWritable.class);
		Option optVal = SequenceFile.Writer.valueClass(BytesWritable.class);

		Option optPath = SequenceFile.Writer.file(outputPath);
		System.out.println("outputPath  :"+outputPath.toUri().getPath());
		writer = SequenceFile.createWriter(conf, optPath, optKey, optVal, optCom);
		setStagingFileGroup();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(D record) {
		try {
			Preconditions.checkNotNull(record);
			BytesWritable key = new BytesWritable();
			BytesWritable value = new BytesWritable();
			key.set(((Tuple<byte[], byte[]>) record).getKey(), 0, ((Tuple<byte[], byte[]>) record).getKey().length);
			value.set(((Tuple<byte[], byte[]>) record).getValue(), 0,
					((Tuple<byte[], byte[]>) record).getValue().length);

			writer.append(key, value);

			// Only increment when write is successful
			this.count.incrementAndGet();
		} catch (IOException e) {
			LOG.error("IOException occured %s ", e);
		}
	}

	@Override
	public void close() throws IOException {
		writer.close();

		if (this.shouldIncludeRecordCountInFileName) {
			String filePathWithRecordCount = addRecordCountToFileName();
			this.properties.appendToSetProp(this.allOutputFilesPropName, filePathWithRecordCount);
		} else {
			this.properties.appendToSetProp(this.allOutputFilesPropName, getOutputFilePath());
		}
	}

	@Override
	public State getFinalState() {
		State state = new State();

		state.setProp("RecordsWritten", recordsWritten());
		try {
			state.setProp("BytesWritten", bytesWritten());
		} catch (Exception exception) {
			// If Writer fails to return bytesWritten, it might not be
			// implemented, or implemented incorrectly.
			// Omit property instead of failing.
		}

		return state;
	}

	/**
	 * Set the group name of the staging output file.
	 *
	 * @throws IOException
	 *             if it fails to set the group name
	 */
	protected void setStagingFileGroup() throws IOException {
		Preconditions.checkArgument(this.fs.exists(this.stagingFile),
				String.format("Staging output file %s does not exist", this.stagingFile));
		if (this.group.isPresent()) {
			HadoopUtils.setGroup(this.fs, this.stagingFile, this.group.get());
		}
	}

	/**
	 * {@inheritDoc}.
	 *
	 * <p>
	 * This default implementation simply renames the staging file to the output
	 * file. If the output file already exists, it will delete it first before
	 * doing the renaming.
	 * </p>
	 *
	 * @throws IOException
	 *             if any file operation fails
	 */
	@Override
	public void commit() throws IOException {

		if (!this.fs.exists(this.stagingFile)) {
			throw new IOException(String.format("File %s does not exist", this.stagingFile));
		}

		// Double check permission of staging file
		if (!this.fs.getFileStatus(this.stagingFile).getPermission().equals(this.filePermission)) {
			this.fs.setPermission(this.stagingFile, this.filePermission);
		}

		LOG.info(String.format("Moving data from %s to %s", this.stagingFile, this.outputFile));
		// For the same reason as deleting the staging file if it already
		// exists, deleting
		// the output file if it already exists prevents task retry from being
		// blocked.
		if (this.fs.exists(this.outputFile)) {
			LOG.warn(String.format("Task output file %s already exists", this.outputFile));
			HadoopUtils.deletePath(this.fs, this.outputFile, false);
		}

		HadoopUtils.renamePath(this.fs, this.stagingFile, this.outputFile);
	}

	/**
	 * {@inheritDoc}.
	 *
	 * <p>
	 * This default implementation simply deletes the staging file if it exists.
	 * </p>
	 *
	 * @throws IOException
	 *             if deletion of the staging file fails
	 */
	@Override
	public void cleanup() throws IOException {
		// Delete the staging file
		if (this.fs.exists(this.stagingFile)) {
			HadoopUtils.deletePath(this.fs, this.stagingFile, false);
		}
	}

	private synchronized String addRecordCountToFileName() throws IOException {
		String filePath = getOutputFilePath();
		String filePathWithRecordCount = IngestionRecordCountProvider.constructFilePath(filePath, recordsWritten());
		LOG.info("Renaming " + filePath + " to " + filePathWithRecordCount);
		HadoopUtils.renamePath(this.fs, new Path(filePath), new Path(filePathWithRecordCount));
		this.outputFile = new Path(filePathWithRecordCount);
		return filePathWithRecordCount;
	}

	/**
	 * Get the output file path.
	 *
	 * @return the output file path
	 */
	public String getOutputFilePath() {
		return this.outputFile.toString();
	}

	@Override
	public long recordsWritten() {
		return this.count.get();
	}

	@Override
	public synchronized long bytesWritten() throws IOException {
		if (!this.fs.exists(this.outputFile)) {
			return 0;
		}

		LOG.info("Output file : " + this.outputFile);
		return this.fs.getFileStatus(this.outputFile).getLen();
	}

}
