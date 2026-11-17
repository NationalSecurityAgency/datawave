package datawave.query.testframework;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.input.reader.event.EventSequenceFileRecordReader;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.query.MockAccumuloRecordWriter;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;

/**
 * Jupiter class-level extension for loading Accumulo test data. An instance of this extension can be created with {@code @RegisterExtension}:
 *
 * <pre>
 * &#64;RegisterExtension
 * public static AccumuloSetupExtension accumuloSetup = AccumuloSetupExtension.create();
 * </pre>
 *
 * The use of {@code @ExtendWith} is not supported for this extension due to the need to store and access stateful information.
 */
public class AccumuloSetupExtension implements BeforeAllCallback, AfterAllCallback {

    private static final Logger log = Logger.getLogger(AccumuloSetupExtension.class);

    /**
     * Whether to cause tests to fail with an {@link org.opentest4j.AssertionFailedError} if resources are left undeleted after the {@code @AfterAll} lifecycle
     * step.
     */
    private final boolean enforceCleanup;

    /**
     * The folder where any generated sequence files will be stored. This folder will be deleted at the end of tests.
     */
    private java.nio.file.Path tempFolder;

    private MockAccumuloRecordWriter recordWriter;
    private Collection<DataTypeHadoopConfig> dataTypes;
    private Set<String> shardIds;
    private FileType fileFormat;
    private Authorizations auths = AbstractDataTypeConfig.getTestAuths();

    /**
     * Return a new instance of {@link AccumuloSetupExtension} that will not cause test failures if resources are left undeleted after the {@code @AfterAll}
     * lifecycle step.
     *
     * @return the new {@link AccumuloSetupExtension}
     */
    public static AccumuloSetupExtension create() {
        return new AccumuloSetupExtension(false);
    }

    /**
     * Return a new {@link AccumuloSetupExtension}.
     *
     * @param enforceCleanup
     *            whether to fail tests with an {@link org.opentest4j.AssertionFailedError} if resources are left undeleted after the {@code @AfterAll}
     *            lifecycle step.
     * @return the new {@link AccumuloSetupExtension}
     */
    public static AccumuloSetupExtension create(boolean enforceCleanup) {
        return new AccumuloSetupExtension(enforceCleanup);
    }

    /**
     * The private constructor here is deliberate to prevent JUnit from instantiating via {@code @ExtendWith}. This is necessary to safeguard against
     * potentially leaking data between tests since we do not use an {@link ExtensionContext.Store} in this class. Instead, we want to force the use of
     * {@code @RegisterExtension}, where an instance of this extension of is always created as an instance or field of the test.
     */
    @SuppressWarnings("unused")
    private AccumuloSetupExtension() {
        this(false);
    }

    /**
     * Returns a new instance of {@link AccumuloSetupExtension}.
     *
     * @param enforceCleanup
     *            whether to fail tests with an {@link org.opentest4j.AssertionFailedError} if resources are left undeleted after the {@code @AfterAll}
     *            lifecycle step.
     */
    private AccumuloSetupExtension(boolean enforceCleanup) {
        this.enforceCleanup = enforceCleanup;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Create a temporary root folder.
        try {
            tempFolder = Files.createTempDirectory(UUID.randomUUID().toString());
        } catch (IOException e) {
            fail("Failed to create temporary directory", e);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        // If the temporary root folder exists, delete it.
        if (tempFolder != null && Files.exists(tempFolder)) {
            try {
                FileUtils.deleteDirectory(tempFolder.toFile());
            } catch (IOException e) {
                if (enforceCleanup) {
                    fail("Failed to clean up temporary directory " + tempFolder, e);
                } else {
                    log.warn("Failed to clean up temporary directory " + tempFolder, e);
                }
            }
        }
    }

    public void setData(FileType fileFormat, DataTypeHadoopConfig config) {
        setData(fileFormat, Collections.singletonList(config));
    }

    public void setData(FileType format, Collection<DataTypeHadoopConfig> types) {
        this.recordWriter = new MockAccumuloRecordWriter();
        this.dataTypes = types;
        this.shardIds = new HashSet<>();
        this.fileFormat = format;
        for (DataTypeHadoopConfig config : this.dataTypes) {
            this.shardIds.addAll(config.getShardIds());
        }
    }

    /** Override the default authorizations used for printing the contents of tables */
    public void setAuthorizations(Authorizations auths) {
        this.auths = auths;
    }

    /**
     * Creates the Accumulo shard ids and ingests the data into the tables. Uses a CSV file for loading test data.
     *
     * @param parentLog
     *            log of parent
     * @return connector to Accumulo
     * @throws AccumuloException
     *             , AccumuloSecurityException, IOException, InterruptedException, TableExistsException, TableNotFoundException Accumulo error conditions
     */
    public AccumuloClient loadTables(final Logger parentLog) throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException,
                    TableExistsException, TableNotFoundException, URISyntaxException {
        return loadTables(parentLog, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
    }

    /**
     * Creates the Accumulo shard ids and ingests the data into the tables. Uses a CSV file for loading test data.
     *
     * @param parentLog
     *            log of parent
     * @return connector to Accumulo
     * @throws AccumuloException
     *             Accumulo error conditions
     * @throws AccumuloSecurityException
     *             Accumulo error conditions
     * @throws IOException
     *             Accumulo error conditions
     * @throws InterruptedException
     *             Accumulo error conditions
     * @throws TableExistsException
     *             Accumulo error conditions
     * @throws TableNotFoundException
     *             Accumulo error conditions
     */
    public AccumuloClient loadTables(final Logger parentLog, final RebuildingScannerTestHelper.TEARDOWN teardown,
                    RebuildingScannerTestHelper.INTERRUPT interrupt) throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException,
                    TableExistsException, TableNotFoundException, URISyntaxException {
        final QueryTestTableHelper tableHelper = new QueryTestTableHelper(this.getClass().getName(), parentLog, teardown, interrupt);
        return this.loadTables(tableHelper);
    }

    /**
     * Creates the Accumulo shard ids and ingests the data into the tables. Uses a CSV file for loading test data.
     *
     * @param tableHelper
     *            the table helper to use
     * @return connector to Accumulo
     * @throws AccumuloException
     *             Accumulo error conditions
     * @throws AccumuloSecurityException
     *             Accumulo error conditions
     * @throws IOException
     *             Accumulo error conditions
     * @throws InterruptedException
     *             Accumulo error conditions
     * @throws TableExistsException
     *             Accumulo error conditions
     * @throws TableNotFoundException
     *             Accumulo error conditions
     */
    public AccumuloClient loadTables(final QueryTestTableHelper tableHelper) throws AccumuloException, AccumuloSecurityException, IOException,
                    InterruptedException, TableExistsException, TableNotFoundException, URISyntaxException {
        log.debug("------------- loadTables -------------");

        if (this.fileFormat != FileType.GROUPING) {
            assertFalse(this.shardIds.isEmpty(), "shard ids have not been specified");
            assertFalse(this.dataTypes.isEmpty(), "data types have not been specified");
        }

        final AccumuloClient client = tableHelper.client;
        tableHelper.configureTables(this.recordWriter);

        for (DataTypeHadoopConfig dataType : this.dataTypes) {
            HadoopTestConfiguration hadoopConfig = new HadoopTestConfiguration(dataType);
            TestFileLoader loader;
            if (this.fileFormat.hasFileLoader()) {
                loader = this.fileFormat.getFileLoader(dataType.getIngestFile(), hadoopConfig);
                ingestTestData(hadoopConfig, loader);
            }
        }

        // The metadata table the query logic is configured with, not the model table that happens to
        // carry TableName.METADATA's name here. Shards per day matches ShardedDataTypeHandler.NUM_SHARDS.
        try (BatchWriter bw = client.createBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME)) {
            Mutation mutation = new Mutation("num_shards");
            mutation.put("ns", "20000101_1", new Value());
            bw.addMutation(mutation);
        }

        tableHelper.printTables(auths);

        return client;
    }

    private void ingestTestData(Configuration conf, TestFileLoader loader) throws IOException, InterruptedException {
        log.debug("------------- ingestTestData -------------");

        Path tmpPath = new Path(tempFolder.toUri());
        // To prevent periodic test cases failing, added "---" prefix for UUID for test cases to support queries with _ANYFIELD_ starting with particular
        // letters.
        Path seqFile = new Path(tmpPath, "---" + UUID.randomUUID());

        TaskAttemptID id = new TaskAttemptID("testJob", 0, TaskType.MAP, 0, 0);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, id);

        try (final RawLocalFileSystem fileSystem = createSequenceFile(conf, seqFile, loader)) {
            InputSplit split = new FileSplit(seqFile, 0, fileSystem.pathToFile(seqFile).length(), null);
            EventSequenceFileRecordReader<LongWritable> recordReader = new EventSequenceFileRecordReader<>();
            recordReader.initialize(split, context);

            Path outputPath = new Path(tmpPath, "oc");
            OutputCommitter outputCommitter = new FileOutputCommitter(outputPath, context);
            fileSystem.deleteOnExit(outputPath);

            StandaloneStatusReporter statusReporter = new StandaloneStatusReporter();
            EventMapper<LongWritable,RawRecordContainer,Text,Mutation> mapper = new EventMapper<>();
            MapContext<LongWritable,RawRecordContainer,Text,Mutation> mapContext = new MapContextImpl<>(conf, id, recordReader, this.recordWriter,
                            outputCommitter, statusReporter, split);
            Mapper<LongWritable,RawRecordContainer,Text,Mutation>.Context mapperContext = new WrappedMapper<LongWritable,RawRecordContainer,Text,Mutation>()
                            .getMapContext(mapContext);
            mapper.run(mapperContext);
            mapper.cleanup(mapperContext);
        } finally {
            this.recordWriter.close(context);
        }
    }

    private RawLocalFileSystem createSequenceFile(Configuration conf, Path path, TestFileLoader loader) throws IOException {
        RawLocalFileSystem fileSystem = new RawLocalFileSystem();
        fileSystem.setConf(conf);
        try (SequenceFile.Writer seqWriter = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(Text.class),
                        SequenceFile.Writer.valueClass(RawRecordContainerImpl.class))) {
            loader.loadTestData(seqWriter);
            return fileSystem;
        }
    }
}
