package datawave.ingest.json.mr.handler;

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

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.json.config.helper.JsonDataTypeHelper;
import datawave.ingest.json.config.helper.JsonIngestHelper;
import datawave.ingest.json.mr.input.JsonRecordReader;
import datawave.ingest.mapreduce.handler.edge.ProtobufEdgeDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.table.constants.TableName;

public class ContentJsonColumnBasedHandlerTest {

    /**
     * If you want to print keys on success for whatever reason, set to false
     */
    private static final boolean PRINT_GENERATED_KEYS_ONLY_ON_FAIL = true;

    private Configuration conf;
    private static final Path edgeKeyVersionCachePath = Paths.get(System.getProperty("user.dir"), "edge-key-version.txt");
    private static final Logger log = LoggerFactory.getLogger(ContentJsonColumnBasedHandlerTest.class);
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

    private JsonRecordReader getJsonRecordReader(String file) throws IOException, URISyntaxException {
        InputSplit split = ColumnBasedHandlerTestUtilJUnit5.getSplit(file);
        TaskAttemptContext ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        if (log.isDebugEnabled()) {
            log.debug(String.valueOf(TypeRegistry.getContents()));
        }
        JsonRecordReader reader = new JsonRecordReader();
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
        conf.setInt(ShardedDataTypeHandler.NUM_SHARDS, 1);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
        conf.set(ShardedDataTypeHandler.SHARD_DAY_INDEX_TABLE_NAME, TableName.SHARD_DAY_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_YEAR_INDEX_TABLE_NAME, TableName.SHARD_YEAR_INDEX);
    }

    @Test
    public void testJsonContentHandlers() throws Exception {
        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/tvmaze-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/edge-ingest-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/metadata-config.xml"));
        TypeRegistry.getInstance(conf);
        JsonDataTypeHelper helper = new JsonDataTypeHelper();
        helper.setup(conf);

        // Set up the IngestHelper
        JsonIngestHelper ingestHelper = new JsonIngestHelper();
        ingestHelper.setup(conf);

        // Set up the ColumnBasedHandler
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        ContentJsonColumnBasedHandler<Text> jsonHandler = new ContentJsonColumnBasedHandler<>();
        jsonHandler.setup(context);

        // Set up the Reader
        JsonRecordReader reader = getJsonRecordReader("/input/tvmaze-seinfeld.json");

        assertTrue(reader.nextKeyValue(), "First Record did not read properly?");
        RawRecordContainer event = reader.getEvent();
        assertNotNull(event, "Event 1 was null.");
        assertTrue(event.getErrors().isEmpty(), "Event 1 has parsing errors");

        // Set up the edge
        ProtobufEdgeDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler = new ProtobufEdgeDataTypeHandler<>();
        edgeHandler.setup(context);

        ColumnBasedHandlerTestUtilJUnit5.processEvent(jsonHandler, edgeHandler, event, 231, 90, 4, 38, PRINT_GENERATED_KEYS_ONLY_ON_FAIL);

        reader.close();
    }
}
