package datawave.ingest.csv.mr.handler;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.csv.config.helper.ExtendedCSVHelper;
import datawave.ingest.csv.config.helper.ExtendedCSVIngestHelper;
import datawave.ingest.csv.mr.input.CSVRecordReader;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.edge.ProtobufEdgeDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.table.constants.TableName;

public class ContentCSVIndexingColumnBasedHandlerTest {

    private Configuration conf;
    private static final Path edgeKeyVersionCachePath = Paths.get(System.getProperty("user.dir"), "edge-key-version.txt");
    private static final Logger log = LoggerFactory.getLogger(ContentCSVIndexingColumnBasedHandlerTest.class);
    private static final LoggerContext logContext = (LoggerContext) LogManager.getContext(false);
    private static final Map<String,Appender> rootAppenders = new HashMap<>(logContext.getRootLogger().getAppenders());

    @BeforeAll
    public static void setupSystemSettings() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", "UTF8");
        // Set edge version cache file...
        try {
            Files.write(edgeKeyVersionCachePath, "1\t1970-01-01T00:00:00.000Z".getBytes());
        } catch (IOException io) {
            log.error("Could not create " + edgeKeyVersionCachePath);
        }
    }

    @AfterAll
    public static void tearDown() {
        logContext.getRootLogger().getAppenders().values().forEach(logContext.getRootLogger()::removeAppender);
        rootAppenders.values().forEach(logContext.getRootLogger()::addAppender);
        try {
            Files.deleteIfExists(edgeKeyVersionCachePath);
        } catch (IOException io) {
            log.error("Could not delete " + edgeKeyVersionCachePath);
        }
    }

    private CSVRecordReader getCSVRecordReader(String file) throws IOException, URISyntaxException {
        InputSplit split = ColumnBasedHandlerTestUtilJUnit5.getSplit(file);
        TaskAttemptContext ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        if (log.isDebugEnabled()) {
            log.debug(String.valueOf(TypeRegistry.getContents()));
        }
        CSVRecordReader reader = new CSVRecordReader();
        reader.initialize(split, ctx);
        return reader;
    }

    private static void enableLogging() {
        logContext.getRootLogger().getAppenders().values().forEach(logContext.getRootLogger()::removeAppender);
        ConsoleAppender ca = ConsoleAppender.newBuilder().setName("TestConsole").setLayout(PatternLayout.newBuilder().withPattern("%p [%c{1}] %m%n").build())
                        .build();
        ca.start();
        logContext.getRootLogger().addAppender(ca);
        Configurator.setLevel(log.getName(), Level.TRACE);
        Configurator.setLevel(ColumnBasedHandlerTestUtilJUnit5.class.getName(), Level.TRACE);
        Configurator.setLevel(ContentIndexingColumnBasedHandler.class.getName(), Level.TRACE);
        Configurator.setLevel(ContentBaseIngestHelper.class.getName(), Level.TRACE);
    }

    private static void disableLogging() {
        Configurator.setLevel(log.getName(), Level.OFF);
        Configurator.setLevel(ColumnBasedHandlerTestUtilJUnit5.class.getName(), Level.OFF);
        Configurator.setLevel(ContentIndexingColumnBasedHandler.class.getName(), Level.OFF);
        Configurator.setLevel(ContentBaseIngestHelper.class.getName(), Level.OFF);
    }

    @BeforeEach
    public void setup() {
        TypeRegistry.reset();
        conf = new Configuration();
        conf.setInt(ShardedDataTypeHandler.NUM_SHARDS, 131);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
        conf.set(ShardedDataTypeHandler.SHARD_DAY_INDEX_TABLE_NAME, TableName.SHARD_DAY_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_YEAR_INDEX_TABLE_NAME, TableName.SHARD_YEAR_INDEX);
        conf.set(ShardedDataTypeHandler.BITSET_INDEX_ENABLED, TableName.TRUNCATED_SHARD_INDEX);
    }

    @Test
    public void testCsv01() throws Exception {
        log.debug("---testCsv01---");
        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/csv-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/edge-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/metadata-config.xml"));
        TypeRegistry.getInstance(conf);
        ExtendedCSVHelper helper = new ExtendedCSVHelper();
        helper.setup(conf);

        // Set up the IngestHelper
        ExtendedCSVIngestHelper ingestHelper = new ExtendedCSVIngestHelper();
        ingestHelper.setup(conf);

        // Set up the ColumnBasedHandler
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        ContentCSVColumnBasedHandler<Text> csvHandler = new ContentCSVColumnBasedHandler<>();
        csvHandler.setup(context);

        // Set up the Reader
        CSVRecordReader reader = getCSVRecordReader("/input/my.csv");

        // ----------------------------------------------------------------------
        // EVENT 1
        assertTrue(reader.nextKeyValue(), "First Record did not read properly?");
        RawRecordContainer event = reader.getEvent();
        assertNotNull(event, "Event 1 was null.");
        assertTrue(event.getErrors().isEmpty(), "Event 1 has parsing errors");

        // Set up the edge
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        edgeHandler.setup(context);

        ColumnBasedHandlerTestUtilJUnit5.processEvent(csvHandler, edgeHandler, event, 73, 29, 25, 4, true);

        // ----------------------------------------------------------------------
        // EVENT 2
        assertTrue(reader.nextKeyValue(), "Second Record did not read properly?");
        event = reader.getEvent();
        assertNotNull(event, "Event 2 was null.");
        assertTrue(event.getErrors().isEmpty(), "Event 2 has parsing errors");

        edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        edgeHandler.setup(context);

        ColumnBasedHandlerTestUtilJUnit5.processEvent(csvHandler, edgeHandler, event, 77, 31, 24, 4, true);

        reader.close();
    }
}
