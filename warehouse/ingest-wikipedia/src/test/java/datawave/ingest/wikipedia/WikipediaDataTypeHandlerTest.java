package datawave.ingest.wikipedia;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.VirtualIngest;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.StandaloneTaskAttemptContext;

import datawave.util.TableName;
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
        
        Multimap<String,NormalizedContentInterface> virtualFields = ((VirtualIngest) helper).getVirtualFields(eventFields);
        for (Entry<String,NormalizedContentInterface> v : virtualFields.entries()) {
            eventFields.put(v.getKey(), v.getValue());
        }
        
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
        
        Assert.assertEquals(82, tableToKey.get(TableName.SHARD).size());
        Assert.assertEquals(38, tableToKey.get(TableName.SHARD_INDEX).size());
        Assert.assertEquals(25, tableToKey.get(TableName.SHARD_RINDEX).size());
        
        // These are only the *_TERM_COUNT things. The rest are handled via EventMapper's EventMetadata instance
        int numberOfLoadDateEntries = 14;
        int numberOfDatawaveMetadataEntries = 18;
        Assert.assertEquals(numberOfDatawaveMetadataEntries, tableToKey.get(TableName.METADATA).size());
        
        Assert.assertEquals(137 + numberOfDatawaveMetadataEntries + numberOfLoadDateEntries, results.size());
        
        contextWriter = new MyCachingContextWriter();
        
        // ///////////////
        
        Assert.assertTrue(reader.nextKeyValue());
        
        e = reader.getEvent();
        
        eventFields = helper.getEventFields(e);
        virtualFields = ((VirtualIngest) helper).getVirtualFields(eventFields);
        for (Entry<String,NormalizedContentInterface> v : virtualFields.entries()) {
            eventFields.put(v.getKey(), v.getValue());
        }
        
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
        
        Assert.assertEquals(9785, tableToKey.get(TableName.SHARD).size());
        Assert.assertEquals(4890, tableToKey.get(TableName.SHARD_INDEX).size());
        Assert.assertEquals(4877, tableToKey.get(TableName.SHARD_RINDEX).size());
        
        // These are only the *_TERM_COUNT things. The rest are handled via EventMapper's EventMetadata instance
        Assert.assertEquals(numberOfDatawaveMetadataEntries, tableToKey.get(TableName.METADATA).size());
        
        Assert.assertEquals(22766 + numberOfDatawaveMetadataEntries + numberOfLoadDateEntries, results.size());
    }
    
}
