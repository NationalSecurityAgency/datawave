package datawave.query.testframework;

import datawave.helpers.PrintUtility;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.input.reader.event.EventSequenceFileRecordReader;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.query.MockAccumuloRecordWriter;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.util.TableName;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.data.Mutation;
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
import org.junit.Assert;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class AccumuloSetup extends ExternalResource {
    
    private static final Logger log = Logger.getLogger(AccumuloSetup.class);
    
    /**
     * Setting this flag assures that no resources are left undeleted. Failure to fulfill the assurance results in failure of tests with an AssertionError.
     */
    private final boolean assureTempFolderDeletion;
    
    /**
     * The folder where any generated sequence files will be stored. This folder will be deleted at the end of tests.
     */
    private File tempFolder;
    
    private MockAccumuloRecordWriter recordWriter;
    private Collection<DataTypeHadoopConfig> dataTypes;
    private Set<String> shardIds;
    private FileType fileFormat;
    private Authorizations auths = AbstractDataTypeConfig.getTestAuths();
    
    public AccumuloSetup() {
        this(false);
    }
    
    public AccumuloSetup(boolean assureTempFolderDeletion) {
        this.assureTempFolderDeletion = assureTempFolderDeletion;
    }
    
    @Override
    protected void before() throws Throwable {
        createTempFolder();
    }
    
    private void createTempFolder() {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        tempFolder = new File(tmpDir, UUID.randomUUID().toString());
        if (!tempFolder.mkdir()) {
            Assert.fail("Failed to create temporary folder " + tempFolder);
        }
    }
    
    @Override
    protected void after() {
        deleteTempFolder();
    }
    
    private void deleteTempFolder() {
        if (!tryDelete()) {
            if (assureTempFolderDeletion) {
                Assert.fail("Failed to delete temp folder " + tempFolder);
            }
        }
    }
    
    private boolean tryDelete() {
        if (tempFolder == null) {
            return true;
        }
        return recursiveDelete(tempFolder);
    }
    
    private boolean recursiveDelete(File file) {
        // Try deleting file before assuming file is a directory to prevent following symbolic links.
        if (file.delete()) {
            return true;
        }
        File[] files = file.listFiles();
        if (files != null) {
            for (File each : files) {
                if (!recursiveDelete(each)) {
                    return false;
                }
            }
        }
        return file.delete();
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
    public Connector loadTables(final Logger parentLog) throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException,
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
     *             , AccumuloSecurityException, IOException, InterruptedException, TableExistsException, TableNotFoundException Accumulo error conditions
     */
    public Connector loadTables(final Logger parentLog, final RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt)
                    throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException, TableExistsException, TableNotFoundException,
                    URISyntaxException {
        log.debug("------------- loadTables -------------");
        
        if (this.fileFormat != FileType.GROUPING) {
            Assert.assertFalse("shard ids have not been specified", this.shardIds.isEmpty());
            Assert.assertFalse("data types have not been specified", this.dataTypes.isEmpty());
        }
        
        QueryTestTableHelper tableHelper = new QueryTestTableHelper(AccumuloSetup.class.getName(), parentLog, teardown, interrupt);
        final Connector connector = tableHelper.connector;
        tableHelper.configureTables(this.recordWriter);
        
        for (DataTypeHadoopConfig dt : this.dataTypes) {
            HadoopTestConfiguration hadoopConfig = new HadoopTestConfiguration(dt);
            TestFileLoader loader;
            if (this.fileFormat.hasFileLoader()) {
                loader = this.fileFormat.getFileLoader(dt.getIngestFile(), hadoopConfig);
                ingestTestData(hadoopConfig, loader);
            }
        }
        
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
        PrintUtility.printTable(connector, auths, TableName.SHARD);
        PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(connector, auths, TableName.SHARD_RINDEX);
        
        // TODO: elsewhere?
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.FACET_TABLE_NAME);
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.FACET_METADATA_TABLE_NAME);
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.FACET_HASH_TABLE_NAME);
        
        return connector;
    }
    
    private void ingestTestData(Configuration conf, TestFileLoader loader) throws IOException, InterruptedException {
        log.debug("------------- ingestTestData -------------");
        
        Path tmpPath = new Path(tempFolder.toURI());
        // To prevent periodic test cases failing, added "---" prefix for UUID for test cases to support queries with _ANYFIELD_ starting with particular
        // letters.
        Path seqFile = new Path(tmpPath, "---" + UUID.randomUUID().toString());
        
        TaskAttemptID id = new TaskAttemptID("testJob", 0, TaskType.MAP, 0, 0);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, id);
        
        try (final RawLocalFileSystem rfs = createSequenceFile(conf, seqFile, loader)) {
            InputSplit split = new FileSplit(seqFile, 0, rfs.pathToFile(seqFile).length(), null);
            EventSequenceFileRecordReader<LongWritable> rr = new EventSequenceFileRecordReader<>();
            rr.initialize(split, context);
            
            Path ocPath = new Path(tmpPath, "oc");
            OutputCommitter oc = new FileOutputCommitter(ocPath, context);
            rfs.deleteOnExit(ocPath);
            
            StandaloneStatusReporter sr = new StandaloneStatusReporter();
            EventMapper<LongWritable,RawRecordContainer,Text,Mutation> mapper = new EventMapper<>();
            MapContext<LongWritable,RawRecordContainer,Text,Mutation> mapContext = new MapContextImpl<>(conf, id, rr, this.recordWriter, oc, sr, split);
            
            Mapper<LongWritable,RawRecordContainer,Text,Mutation>.Context con = new WrappedMapper<LongWritable,RawRecordContainer,Text,Mutation>()
                            .getMapContext(mapContext);
            mapper.run(con);
            mapper.cleanup(con);
        } finally {
            this.recordWriter.close(context);
        }
    }
    
    private RawLocalFileSystem createSequenceFile(Configuration conf, Path path, TestFileLoader loader) throws IOException {
        RawLocalFileSystem rfs = new RawLocalFileSystem();
        rfs.setConf(conf);
        
        try (SequenceFile.Writer seqWriter = new SequenceFile.Writer(rfs, conf, path, Text.class, RawRecordContainerImpl.class)) {
            loader.loadTestData(seqWriter);
            return rfs;
        }
    }
}
