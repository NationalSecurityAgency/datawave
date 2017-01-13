package nsa.datawave.ingest.wikipedia;

import java.io.IOException;
import java.util.Map;

import nsa.datawave.ingest.data.RawRecordContainer;
import nsa.datawave.ingest.data.config.NormalizedContentInterface;
import nsa.datawave.ingest.mapreduce.job.BulkIngestKey;
import nsa.datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import nsa.datawave.poller.manager.mapreduce.StandaloneStatusReporter;
import nsa.datawave.poller.manager.mapreduce.StandaloneTaskAttemptContext;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * 
 */
@RunWith(JUnit4.class)
public class WikipediaDataTypeHandlerTest extends WikipediaTestBed {
    
    protected static WikipediaDataTypeHandler<Text,BulkIngestKey,Value> handler = null;
    
    public class MyCachingContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
        private Multimap<BulkIngestKey,Value> cache = HashMultimap.create();
        
        @Override
        protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) throws IOException,
                        InterruptedException {
            for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
                cache.put(entry.getKey(), entry.getValue());
            }
        }
        
        public Multimap<BulkIngestKey,Value> getCache() {
            return cache;
        }
    }
    
    @BeforeClass
    public static void before() throws Exception {
        handler = new WikipediaDataTypeHandler<>();
        System.setProperty("LOADDATES_TABLE_ENABLED", "true");
        System.setProperty("LOADDATES_TABLE_NAME", "LoadDatesTable");
    }
    
    @Test
    public void testWiki1() throws Exception {
        WikipediaRecordReader reader = new WikipediaRecordReader();
        reader.initialize(split, ctx);
        reader.setInputDate(System.currentTimeMillis());
        
        Assert.assertTrue(reader.nextKeyValue());
        
        RawRecordContainer e = reader.getEvent();
        
        handler.setup(ctx);
        
        WikipediaIngestHelper helper = new WikipediaIngestHelper();
        helper.setup(conf);
        
        Multimap<String,NormalizedContentInterface> eventFields = helper.getEventFields(e);
        // create a context writer that stores up the results
        MyCachingContextWriter contextWriter = new MyCachingContextWriter();
        
        // Call the Handler
        StandaloneTaskAttemptContext<Text,RawRecordContainer,BulkIngestKey,Value> ctx = new StandaloneTaskAttemptContext<>(conf, new StandaloneStatusReporter());
        handler.process(new Text("1"), e, eventFields, ctx, contextWriter);
        try {
            contextWriter.write(handler.getMetadata().getBulkMetadata(), ctx);
        } finally {
            contextWriter.commit(ctx);
        }
        
        Multimap<BulkIngestKey,Value> results = contextWriter.getCache();
        Multimap<String,BulkIngestKey> tableToKey = HashMultimap.create();
        
        for (BulkIngestKey biKey : results.keySet()) {
            tableToKey.put(biKey.getTableName().toString(), biKey);
        }
        
        Assert.assertEquals(76, tableToKey.get("shard").size());
        Assert.assertEquals(34, tableToKey.get("shardIndex").size());
        Assert.assertEquals(25, tableToKey.get("shardReverseIndex").size());
        
        // These are only the *_TERM_COUNT things. The rest are handled via EventMapper's EventMetadata instance
        int numberOfLoadDateEntries = 6;
        Assert.assertEquals(16, tableToKey.get("DatawaveMetadata").size());
        
        Assert.assertEquals(151 + numberOfLoadDateEntries, results.size());
        
        contextWriter = new MyCachingContextWriter();
        
        // ///////////////
        
        Assert.assertTrue(reader.nextKeyValue());
        
        e = reader.getEvent();
        
        eventFields = helper.getEventFields(e);
        
        // Call the Handler
        handler.process(new Text("2"), e, eventFields, ctx, contextWriter);
        try {
            contextWriter.write(handler.getMetadata().getBulkMetadata(), ctx);
        } finally {
            contextWriter.commit(ctx);
        }
        
        handler.close(ctx);
        
        results = contextWriter.getCache();
        
        tableToKey = HashMultimap.create();
        
        for (BulkIngestKey biKey : results.keySet()) {
            tableToKey.put(biKey.getTableName().toString(), biKey);
        }
        
        Assert.assertEquals(9799, tableToKey.get("shard").size());
        Assert.assertEquals(4896, tableToKey.get("shardIndex").size());
        Assert.assertEquals(4887, tableToKey.get("shardReverseIndex").size());
        
        // These are only the *_TERM_COUNT things. The rest are handled via EventMapper's EventMetadata instance
        Assert.assertEquals(16, tableToKey.get("DatawaveMetadata").size());
        
        Assert.assertEquals(22812 + numberOfLoadDateEntries, results.size());
    }
    
}
