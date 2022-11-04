package datawave.ingest.nyctlc.mr.handler;

import datawave.ingest.csv.mr.handler.ColumnBasedHandlerTestUtil;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.nyctlc.NYCTLCHelper;
import datawave.ingest.nyctlc.NYCTLCIngestHelper;
import datawave.ingest.nyctlc.NYCTLCReader;
import datawave.util.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.TimeZone;

public class NYCTLCColumnBasedHandlerTest {
    
    private Configuration conf;
    private static Logger log = Logger.getLogger(NYCTLCColumnBasedHandlerTest.class);
    
    @BeforeAll
    public static void setupSystemSettings() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        System.setProperty("file.encoding", "UTF8");
    }
    
    private NYCTLCReader getNYCTLCRecordReader(String file) throws IOException, URISyntaxException {
        InputSplit split = ColumnBasedHandlerTestUtil.getSplit(file);
        TaskAttemptContext ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        TypeRegistry.reset();
        TypeRegistry.getInstance(ctx.getConfiguration());
        log.debug(TypeRegistry.getContents());
        NYCTLCReader reader = new NYCTLCReader();
        reader.initialize(split, ctx);
        return reader;
    }
    
    @BeforeEach
    public void setup() {
        TypeRegistry.reset();
        conf = new Configuration();
        conf.setInt(ShardedDataTypeHandler.NUM_SHARDS, 131);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
    }
    
    @Test
    public void testNyctlc01() throws Exception {
        log.debug("---testNyctlc01---");
        conf.addResource(ClassLoader.getSystemResource("config/ingest/all-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/nyctlc-config.xml"));
        conf.addResource(ClassLoader.getSystemResource("config/ingest/metadata-config.xml"));
        TypeRegistry.getInstance(conf);
        NYCTLCHelper helper = new NYCTLCHelper();
        helper.setup(conf);
        
        // Set up the IngestHelper
        NYCTLCIngestHelper ingestHelper = new NYCTLCIngestHelper();
        ingestHelper.setup(conf);
        
        // Set up the ColumnBasedHandler
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        AbstractColumnBasedHandler<Text> abstractHandler = new AbstractColumnBasedHandler<>();
        abstractHandler.setup(context);
        
        // Set up the Reader
        NYCTLCReader reader = getNYCTLCRecordReader("/input/nyctlc.csv");
        
        // ----------------------------------------------------------------------
        // EVENT 1
        Assertions.assertTrue(reader.nextKeyValue(), "First Record did not read properly?");
        RawRecordContainer event = reader.getEvent();
        Assertions.assertNotNull(event, "Event 1 was null.");
        Assertions.assertTrue(event.getErrors().isEmpty(), "Event 1 has parsing errors");
        System.out.println("================================= EVENT 1 =================================");
        ColumnBasedHandlerTestUtil.processEvent(abstractHandler, null, event, 66, 33, 0, 0, false);
        
        // ----------------------------------------------------------------------
        // EVENT 2
        Assertions.assertTrue(reader.nextKeyValue(), "Second Record did not read properly?");
        event = reader.getEvent();
        Assertions.assertNotNull(event, "Event 2 was null.");
        Assertions.assertTrue(event.getErrors().isEmpty(), "Event 2 has parsing errors");
        System.out.println("================================= EVENT 2 =================================");
        ColumnBasedHandlerTestUtil.processEvent(abstractHandler, null, event, 63, 27, 0, 0, false);
        
        // ----------------------------------------------------------------------
        // EVENT 3
        Assertions.assertTrue(reader.nextKeyValue(), "Third Record did not read properly?");
        event = reader.getEvent();
        Assertions.assertNotNull(event, "Event 3 was null.");
        Assertions.assertTrue(event.getErrors().isEmpty(), "Event 3 has parsing errors");
        System.out.println("================================= EVENT 3 =================================");
        ColumnBasedHandlerTestUtil.processEvent(abstractHandler, null, event, 74, 39, 0, 0, false);
        
        reader.close();
    }
}
