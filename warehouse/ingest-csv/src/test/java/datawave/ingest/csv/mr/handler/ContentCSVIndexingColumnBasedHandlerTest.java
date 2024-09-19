package datawave.ingest.csv.mr.handler;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
import datawave.util.TableName;

public class ContentCSVIndexingColumnBasedHandlerTest {

    private Configuration conf;
    private static Path edgeKeyVersionCachePath = Paths.get(System.getProperty("user.dir"), "edge-key-version.txt");
    private static Logger log = LogManager.getLogger(ContentCSVIndexingColumnBasedHandlerTest.class);
    private static LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    private static org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
    private static LoggerConfig rootLoggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
    private static Appender[] rootAppenders = rootLoggerConfig.getAppenders().values().toArray(new Appender[0]);

    @BeforeClass
    public static void setupSystemSettings() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", "UTF8");
        // Set edge version cache file...
        try {
            Files.write(edgeKeyVersionCachePath, "1\t1970-01-01T00:00:00.000Z".getBytes());
        } catch (IOException io) {
            log.fatal("Could not create " + edgeKeyVersionCachePath);
        }
    }

    @AfterClass
    public static void tearDown() {
        for (Appender appender : rootLoggerConfig.getAppenders().values()) {
            rootLoggerConfig.removeAppender(appender.getName());
        }
        for (Appender appender : rootAppenders) {
            rootLoggerConfig.addAppender(appender, null, null);
        }
        ctx.updateLoggers();
        try {
            Files.deleteIfExists(edgeKeyVersionCachePath);
        } catch (IOException io) {
            log.error("Could not delete " + edgeKeyVersionCachePath);
        }
    }

    private CSVRecordReader getCSVRecordReader(String file) throws IOException, URISyntaxException {
        InputSplit split = ColumnBasedHandlerTestUtil.getSplit(file);
        TaskAttemptContext ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        log.debug(TypeRegistry.getContents());
        CSVRecordReader reader = new CSVRecordReader();
        reader.initialize(split, ctx);
        return reader;
    }

    private static void enableLogging() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();

        ConsoleAppender ca = ConsoleAppender.newBuilder().setLayout(PatternLayout.newBuilder().withPattern("%p [%c{1}] %m%n").build()).setName("Console")
                        .build();
        ca.start();

        LoggerConfig rootLoggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        rootLoggerConfig.addAppender(ca, Level.TRACE, null);
        rootLoggerConfig.setLevel(Level.TRACE);

        config.getLoggerConfig(LogManager.getLogger(ColumnBasedHandlerTestUtil.class).getName()).setLevel(Level.TRACE);
        config.getLoggerConfig(LogManager.getLogger(ContentIndexingColumnBasedHandler.class).getName()).setLevel(Level.TRACE);
        config.getLoggerConfig(LogManager.getLogger(ContentBaseIngestHelper.class).getName()).setLevel(Level.TRACE);

        // Apply the configuration changes
        ctx.updateLoggers();
    }

    private static void disableLogging() {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();

        LoggerConfig rootLoggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        rootLoggerConfig.setLevel(Level.OFF);

        config.getLoggerConfig(LogManager.getLogger(ColumnBasedHandlerTestUtil.class).getName()).setLevel(Level.OFF);
        config.getLoggerConfig(LogManager.getLogger(ContentIndexingColumnBasedHandler.class).getName()).setLevel(Level.OFF);
        config.getLoggerConfig(LogManager.getLogger(ContentBaseIngestHelper.class).getName()).setLevel(Level.OFF);

        ctx.updateLoggers();
    }

    @Before
    public void setup() {
        TypeRegistry.reset();
        conf = new Configuration();
        conf.setInt(ShardedDataTypeHandler.NUM_SHARDS, 131);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
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
        Assert.assertTrue("First Record did not read properly?", reader.nextKeyValue());
        RawRecordContainer event = reader.getEvent();
        Assert.assertNotNull("Event 1 was null.", event);
        Assert.assertTrue("Event 1 has parsing errors", event.getErrors().isEmpty());

        // Set up the edge
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        edgeHandler.setup(context);

        ColumnBasedHandlerTestUtil.processEvent(csvHandler, edgeHandler, event, 73, 29, 25, 4, true);

        // ----------------------------------------------------------------------
        // EVENT 2
        Assert.assertTrue("Second Record did not read properly?", reader.nextKeyValue());
        event = reader.getEvent();
        Assert.assertNotNull("Event 2 was null.", event);
        Assert.assertTrue("Event 2 has parsing errors", event.getErrors().isEmpty());

        edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        edgeHandler.setup(context);

        ColumnBasedHandlerTestUtil.processEvent(csvHandler, edgeHandler, event, 77, 31, 24, 4, true);

        reader.close();
    }
}
