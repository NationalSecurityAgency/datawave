package datawave.ingest.wikipedia;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.StandaloneTaskAttemptContext;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.table.constants.TableName;

/**
 *
 */
public class WikipediaDataTypeHandlerTest extends WikipediaTestBedJUnit5 {

    protected static WikipediaDataTypeHandler<Text,BulkIngestKey,Value> handler = null;

    public static class MyCachingContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
        private final Multimap<BulkIngestKey,Value> cache = HashMultimap.create();

        @Override
        protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context) {
            for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
                cache.put(entry.getKey(), entry.getValue());
            }
        }

        public Multimap<BulkIngestKey,Value> getCache() {
            return cache;
        }
    }

    @BeforeAll
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

        assertTrue(reader.nextKeyValue());

        RawRecordContainer e = reader.getEvent();

        handler.setup(ctx);

        WikipediaIngestHelper helper = new WikipediaIngestHelper();
        helper.setup(conf);

        Multimap<String,NormalizedContentInterface> eventFields = helper.getEventFields(e);
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);
        for (Entry<String,NormalizedContentInterface> v : virtualFields.entries()) {
            eventFields.put(v.getKey(), v.getValue());
        }

        // create a context writer that stores up the results
        MyCachingContextWriter contextWriter = new MyCachingContextWriter();

        // Call the Handler
        StandaloneTaskAttemptContext<Text,RawRecordContainer,BulkIngestKey,Value> ctx = new StandaloneTaskAttemptContext<>(conf,
                        new StandaloneStatusReporter());
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

        assertEquals(82, tableToKey.get(TableName.SHARD).size());
        assertEquals(38, tableToKey.get(TableName.SHARD_INDEX).size());
        assertEquals(25, tableToKey.get(TableName.SHARD_RINDEX).size());

        assertEquals(handler.getDayIndexEnabled(), tableToKey.containsKey(TableName.SHARD_DAY_INDEX));
        if (tableToKey.containsKey(TableName.SHARD_DAY_INDEX)) {
            assertEquals(40, tableToKey.get(TableName.SHARD_DAY_INDEX).size());
        }

        assertEquals(handler.getYearIndexEnabled(), tableToKey.containsKey(TableName.SHARD_YEAR_INDEX));
        if (tableToKey.containsKey(TableName.SHARD_YEAR_INDEX)) {
            assertEquals(25, tableToKey.get(TableName.SHARD_YEAR_INDEX).size());
        }

        // These are only the *_TERM_COUNT things. The rest are handled via EventMapper's EventMetadata instance
        int numberOfLoadDateEntries = 14;
        int numberOfDatawaveMetadataEntries = 18;
        assertEquals(numberOfDatawaveMetadataEntries, tableToKey.get(TableName.METADATA).size());

        int expected = 137 + numberOfDatawaveMetadataEntries + numberOfLoadDateEntries;
        if (handler.getDayIndexEnabled()) {
            expected += 40;
        }
        if (handler.getYearIndexEnabled()) {
            expected += 40;
        }

        assertEquals(expected, results.size());

        contextWriter = new MyCachingContextWriter();

        // ///////////////

        assertTrue(reader.nextKeyValue());

        e = reader.getEvent();

        eventFields = helper.getEventFields(e);
        virtualFields = helper.getVirtualFields(eventFields);
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

        assertEquals(9785, tableToKey.get(TableName.SHARD).size());
        assertEquals(4890, tableToKey.get(TableName.SHARD_INDEX).size());
        assertEquals(4877, tableToKey.get(TableName.SHARD_RINDEX).size());

        assertEquals(handler.getDayIndexEnabled(), tableToKey.containsKey(TableName.SHARD_DAY_INDEX));
        if (tableToKey.containsKey(TableName.SHARD_DAY_INDEX)) {
            assertEquals(5219, tableToKey.get(TableName.SHARD_DAY_INDEX).size());
        }

        assertEquals(handler.getYearIndexEnabled(), tableToKey.containsKey(TableName.SHARD_YEAR_INDEX));
        if (tableToKey.containsKey(TableName.SHARD_YEAR_INDEX)) {
            assertEquals(5204, tableToKey.get(TableName.SHARD_YEAR_INDEX).size());
        }

        // These are only the *_TERM_COUNT things. The rest are handled via EventMapper's EventMetadata instance
        assertEquals(numberOfDatawaveMetadataEntries, tableToKey.get(TableName.METADATA).size());

        int resultSize = results.size();
        expected = 22798;
        if (handler.getDayIndexEnabled()) {
            expected += 5219;
        }
        if (handler.getYearIndexEnabled()) {
            expected += 5219;
        }
        assertEquals(expected, resultSize);
    }

}
