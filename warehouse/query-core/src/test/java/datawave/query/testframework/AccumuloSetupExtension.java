package datawave.query.testframework;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
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
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.input.reader.event.EventSequenceFileRecordReader;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.query.MockAccumuloRecordWriter;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;

/**
 * Jupiter class-level extension for creating {@link AccumuloClient} instances with configured test data. An instance of this extension can be created with
 * {@code @RegisterExtension}:
 *
 * <pre>
 * &#64;RegisterExtension
 * public static AccumuloSetupExtension accumuloSetup = AccumuloSetupExtension.create();
 * </pre>
 *
 * Configuring the test data can be done in a fluent-style manner. At a minimum, the test data must be set via {@code withData(...)}, and the last call must be
 * to {@code @ingest(...)}. For example:
 *
 * <pre>
 * &#64;BeforeAll
 * public static void beforeAll() {
 *     // @formatter:off
 *     accumuloSetup.withData(FileType.CSV, new CitiesDataType(CityEntry.generic, generic))
 *          .withAuthorizations(getAuths())
 *          .withTeardown(RebuildingScannerTestHelper.TEARDOWN.NEVER)
 *          .withInterrupt(RebuildingScannerTestHelper.INTERRUPT.NEVER)
 *          .ingest(log);
 *     // @formatter:on
 * }
 * </pre>
 *
 * The created {@link AccumuloClient} can be fetched via {@link #getClient()} if need be. It will also automatically be injected into any annotated test context
 * methods that accept an {@link AccumuloClient} parameter, e.g.:
 *
 * <pre>
 * &#64;BeforeEach
 * public static void beforeEach(AccumuloClient client) {
 *     ...
 * }
 * </pre>
 *
 * The use of {@code @ExtendWith} is not supported for this extension due to the need to store and access stateful information. The only state kept in an
 * {@link ExtensionContext.Store} is the {@link AccumuloClient}, so it is closed with the test class and can be injected as a parameter.
 */
public class AccumuloSetupExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private static final Logger log = Logger.getLogger(AccumuloSetupExtension.class);
    private static final String REGISTRY_KEY = "accumuloClientRegistry";

    /**
     * Instance-scoped namespace. Even if multiple extension instances are created in the same context, their stores will not collide.
     */
    private final ExtensionContext.Namespace namespace = ExtensionContext.Namespace.create(AccumuloSetupExtension.class);

    /**
     * Whether to cause tests to fail with an {@link org.opentest4j.AssertionFailedError} if resources are left undeleted after the {@code @AfterAll} lifecycle
     * step.
     */
    private final boolean enforceCleanup;

    /**
     * Context of the test class this (static, per-class) extension instance is registered on. Set in {@link #beforeAll(ExtensionContext)}, cleared in
     * {@link #afterAll(ExtensionContext)}.
     */
    private ExtensionContext classContext;

    /**
     * The folder where any generated sequence files will be stored. This folder will be deleted at the end of tests.
     */
    private java.nio.file.Path tempFolder;

    private MockAccumuloRecordWriter recordWriter;
    private Collection<DataTypeHadoopConfig> dataTypes;
    private Set<String> shardIds;
    private FileType fileFormat;
    private RebuildingScannerTestHelper.TEARDOWN teardown = RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER;
    private RebuildingScannerTestHelper.INTERRUPT interrupt = RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER;
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
     * potentially leaking data between tests since most state is held on the instance rather than an {@link ExtensionContext.Store} in this class. Instead, we
     * want to force the use of {@code @RegisterExtension}, where an instance of this extension of is always created as an instance or field of the test.
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

    /**
     * Set up the extension.
     *
     * @param context
     *            the current extension context; never {@code null}
     * @throws Exception
     *             if an error occurs
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Set the context.
        this.classContext = context;

        // Create a temporary root folder.
        try {
            tempFolder = Files.createTempDirectory(UUID.randomUUID().toString());
        } catch (IOException e) {
            fail("Failed to create temporary directory", e);
        }
    }

    /**
     * Clean up the extension.
     *
     * @param context
     *            the current extension context; never {@code null}
     * @throws Exception
     *             if an error occurs
     */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        // Clear the context.
        this.classContext = null;

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

    /**
     * This extension supports resolving parameters that are {@link AccumuloClient} types.
     */
    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == AccumuloClient.class;
    }

    /**
     * This extension will return the current registered {@link AccumuloClient}, if any, for parameters accepting an {@link AccumuloClient} type.
     *
     * @return the client
     * @throws ParameterResolutionException
     *             if no {@link AccumuloClient} was registered.
     */
    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return getClient();
    }

    /**
     * Returns the {@link AccumuloClient}. For any {@code @BeforeAll} code that needs the client after calling {@code ingest(...)}.
     *
     * @return the client
     */
    public AccumuloClient getClient() {
        ClientRegistry registry = classContext.getStore(namespace).get(REGISTRY_KEY, ClientRegistry.class);
        if (registry != null) {
            AccumuloClient client = registry.current().orElse(null);
            if (client != null) {
                return client;
            }
        }
        throw new ParameterResolutionException("No AccumuloClient is available. Call ingest(...) in @BeforeAll first.");
    }

    /**
     * Set the data to ingest.
     *
     * @param fileFormat
     *            the file format
     * @param configs
     *            the datatype configurations
     * @return this {@link AccumuloSetupExtension}
     */
    public AccumuloSetupExtension withData(FileType fileFormat, DataTypeHadoopConfig... configs) {
        return withData(fileFormat, Arrays.asList(configs));
    }

    /**
     * Set the data to ingest.
     *
     * @param fileFormat
     *            the file format
     * @param configs
     *            the datatype configurations
     * @return this {@link AccumuloSetupExtension}
     */
    public AccumuloSetupExtension withData(FileType fileFormat, Collection<DataTypeHadoopConfig> configs) {
        this.recordWriter = new MockAccumuloRecordWriter();
        this.dataTypes = configs;
        this.shardIds = new HashSet<>();
        this.fileFormat = fileFormat;
        for (DataTypeHadoopConfig config : this.dataTypes) {
            this.shardIds.addAll(config.getShardIds());
        }
        return this;
    }

    /**
     * Override the default authorizations.
     *
     * @param auths
     *            the auths
     * @return this {@link AccumuloSetupExtension}
     */
    public AccumuloSetupExtension withAuthorizations(Authorizations auths) {
        this.auths = auths;
        return this;
    }

    /**
     * Set the teardown setting. Defaults to {@link datawave.query.RebuildingScannerTestHelper.TEARDOWN#EVERY_OTHER}.
     *
     * @param teardown
     *            the teardown
     * @return this {@link AccumuloSetupExtension}
     */
    public AccumuloSetupExtension withTeardown(RebuildingScannerTestHelper.TEARDOWN teardown) {
        this.teardown = teardown;
        return this;
    }

    /**
     * Set the interrupt setting. Defaults to {@link datawave.query.RebuildingScannerTestHelper.INTERRUPT#EVERY_OTHER}.
     *
     * @param interrupt
     *            the interrupt
     * @return this {@link AccumuloSetupExtension}
     */
    public AccumuloSetupExtension withInterrupt(RebuildingScannerTestHelper.INTERRUPT interrupt) {
        this.interrupt = interrupt;
        return this;
    }

    /**
     * Create and register an {@link AccumuloClient}, creating the shard IDs and ingesting the test data into the tables.
     *
     * @param parentLog
     *            the parent class logger
     * @return this {@link AccumuloSetupExtension}
     * @throws Exception
     *             if an error occurs.
     * @see #ingest(QueryTestTableHelper)
     */
    public AccumuloSetupExtension ingest(Logger parentLog) throws Exception {
        log.debug("------------- ingest -------------");
        requireConfigured();
        return ingest(new QueryTestTableHelper(getClass().getName(), parentLog, teardown, interrupt));
    }

    /**
     * Create and register an {@link AccumuloClient}, creating the shard IDs and ingesting the test data into the tables.
     *
     * @param helper
     *            the query helper
     * @return this {@link AccumuloSetupExtension}
     * @throws Exception
     *             if an error occurs
     */
    public AccumuloSetupExtension ingest(QueryTestTableHelper helper) throws Exception {
        log.debug("------------- ingest -------------");

        requireConfigured();

        if (this.fileFormat != FileType.GROUPING) {
            assertFalse(this.shardIds.isEmpty(), "shard ids have not been specified");
            assertFalse(this.dataTypes.isEmpty(), "data types have not been specified");
        }

        final AccumuloClient client = helper.client;
        // Immediately register the client before any other steps. This way if a failure occurs, we still close the client.
        registry().register(client);

        helper.configureTables(this.recordWriter);

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

        helper.printTables(auths);

        return this;
    }

    private void requireConfigured() {
        if (classContext == null) {
            throw new IllegalStateException("Extension not initialized: register it with @RegisterExtension on a static field");
        }
        if (fileFormat == null || dataTypes == null || dataTypes.isEmpty()) {
            throw new IllegalStateException("No data configured: call withData(...) before ingest(...)");
        }
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

    /**
     * Return the {@link ClientRegistry} from the context store, creating one if needed.
     *
     * @return the registry
     */
    private ClientRegistry registry() {
        if (classContext == null) {
            throw new IllegalStateException(AccumuloSetupExtension.class.getSimpleName() + " has not been initialized. Register it with @RegisterExtension on"
                            + " a static field.");
        }
        return classContext.getStore(namespace).getOrComputeIfAbsent(REGISTRY_KEY, k -> new ClientRegistry(), ClientRegistry.class);
    }

    /**
     * A registry that tracks every client created for a test class and closes them all when the class' context is closed.
     */
    private static final class ClientRegistry implements ExtensionContext.Store.CloseableResource {

        private final Deque<AccumuloClient> clients = new ArrayDeque<>();

        synchronized void register(AccumuloClient client) {
            if (!clients.contains(client)) {
                clients.push(client);
            }
        }

        synchronized Optional<AccumuloClient> current() {
            return Optional.ofNullable(clients.peek());
        }

        @Override
        public synchronized void close() {
            while (!clients.isEmpty()) {
                try {
                    clients.pop().close();
                } catch (Exception e) {
                    log.warn("Failed to close AccumuloClient", e);
                }
            }
        }
    }
}
