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
import datawave.query.testframework.FileLoaderFactory.FileType;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Base class used to initialize the in memory instance of Accumulo. It can be used by any test cases.
 */
public class AccumuloSetupHelper {
    
    private static final Logger log = Logger.getLogger(AccumuloSetupHelper.class);
    
    private final MockAccumuloRecordWriter recordWriter;
    private final Collection<DataTypeHadoopConfig> dataTypes;
    private final Set<String> shardIds;
    private final FileType fileFormat;
    
    public AccumuloSetupHelper(final Collection<DataTypeHadoopConfig> types) {
        this(types, FileType.CSV);
    }
    
    /**
     * Allows loading of test data into Accumuo using multiple file formats.
     * 
     * @param types
     *            datatypes for loading
     * @param format
     *            file format of data
     */
    public AccumuloSetupHelper(final Collection<DataTypeHadoopConfig> types, FileType format) {
        this.recordWriter = new MockAccumuloRecordWriter();
        this.dataTypes = types;
        this.shardIds = new HashSet<>();
        this.fileFormat = format;
        for (DataTypeHadoopConfig config : this.dataTypes) {
            this.shardIds.addAll(config.getShardIds());
        }
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
        
        QueryTestTableHelper tableHelper = new QueryTestTableHelper(AccumuloSetupHelper.class.getName(), parentLog, teardown, interrupt);
        final Connector connector = tableHelper.connector;
        tableHelper.configureTables(this.recordWriter);
        
        for (DataTypeHadoopConfig dt : this.dataTypes) {
            HadoopTestConfiguration hadoopConfig = new HadoopTestConfiguration(dt);
            TestFileLoader loader;
            switch (this.fileFormat) {
                case CSV:
                    loader = new CSVTestFileLoader(dt.getIngestFile(), hadoopConfig);
                    ingestTestData(hadoopConfig, loader);
                    break;
                case JSON:
                    loader = new JsonTestFileLoader(dt.getIngestFile(), hadoopConfig);
                    ingestTestData(hadoopConfig, loader);
                    break;
                case GROUPING:
                    break;
                default:
                    throw new AssertionError("unknown file format: " + this.fileFormat.name());
            }
        }
        
        PrintUtility.printTable(connector, AbstractDataTypeConfig.getTestAuths(), QueryTestTableHelper.METADATA_TABLE_NAME);
        PrintUtility.printTable(connector, AbstractDataTypeConfig.getTestAuths(), QueryTestTableHelper.SHARD_TABLE_NAME);
        PrintUtility.printTable(connector, AbstractDataTypeConfig.getTestAuths(), QueryTestTableHelper.SHARD_INDEX_TABLE_NAME);
        PrintUtility.printTable(connector, AbstractDataTypeConfig.getTestAuths(), QueryTestTableHelper.SHARD_RINDEX_TABLE_NAME);
        
        return connector;
    }
    
    private void ingestTestData(Configuration conf, TestFileLoader loader) throws IOException, InterruptedException {
        log.debug("------------- ingestTestData -------------");
        
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        Path tmpPath = new Path(tmpDir.toURI());
        Path seqFile = new Path(tmpPath, UUID.randomUUID().toString());
        
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
