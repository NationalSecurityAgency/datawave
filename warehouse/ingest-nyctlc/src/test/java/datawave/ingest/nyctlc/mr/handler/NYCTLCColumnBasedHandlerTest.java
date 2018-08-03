package datawave.ingest.nyctlc.mr.handler;

import datawave.ingest.csv.mr.handler.ColumnBasedHandlerTestUtil;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.nyctlc.NYCTLCHelper;
import datawave.ingest.nyctlc.NYCTLCIngestHelper;
import datawave.ingest.nyctlc.NYCTLCReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.TimeZone;

public class NYCTLCColumnBasedHandlerTest {
    
    private Configuration conf;
    private static Logger log = Logger.getLogger(NYCTLCColumnBasedHandlerTest.class);
    
    @BeforeClass
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
    
    @Before
    public void setup() {
        TypeRegistry.reset();
        conf = new Configuration();
        conf.setInt(ShardedDataTypeHandler.NUM_SHARDS, 131);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, "shard");
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, "shardIndex");
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, "shardReverseIndex");
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
        AbstractColumnBasedHandler<Text> abstractHandler = new AbstractColumnBasedHandler<Text>();
        abstractHandler.setup(context);
        
        // Set up the Reader
        NYCTLCReader reader = getNYCTLCRecordReader("/input/nyctlc.csv");
        
        // ----------------------------------------------------------------------
        // EVENT 1
        Assert.assertTrue("First Record did not read properly?", reader.nextKeyValue());
        RawRecordContainer event = reader.getEvent();
        Assert.assertNotNull("Event 1 was null.", event);
        Assert.assertTrue("Event 1 has parsing errors", event.getErrors().isEmpty());
        System.out.println("================================= EVENT 1 =================================");
        ColumnBasedHandlerTestUtil.processEvent(abstractHandler, null, event, 28, 6, 0, 0, false);
        
        // ----------------------------------------------------------------------
        // EVENT 2
        Assert.assertTrue("Second Record did not read properly?", reader.nextKeyValue());
        event = reader.getEvent();
        Assert.assertNotNull("Event 2 was null.", event);
        Assert.assertTrue("Event 2 has parsing errors", event.getErrors().isEmpty());
        System.out.println("================================= EVENT 2 =================================");
        ColumnBasedHandlerTestUtil.processEvent(abstractHandler, null, event, 29, 6, 0, 0, false);
        
        // ----------------------------------------------------------------------
        // EVENT 3
        Assert.assertTrue("Third Record did not read properly?", reader.nextKeyValue());
        event = reader.getEvent();
        Assert.assertNotNull("Event 3 was null.", event);
        Assert.assertTrue("Event 3 has parsing errors", event.getErrors().isEmpty());
        System.out.println("================================= EVENT 3 =================================");
        ColumnBasedHandlerTestUtil.processEvent(abstractHandler, null, event, 28, 6, 0, 0, false);
        
        reader.close();
    }
}
