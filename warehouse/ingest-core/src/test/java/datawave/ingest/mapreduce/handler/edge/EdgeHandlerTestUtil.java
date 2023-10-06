package datawave.ingest.mapreduce.handler.edge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;
import org.junit.Assert;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.edge.util.EdgeValue;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.ingest.test.StandaloneTaskAttemptContext;
import datawave.util.TableName;

public class EdgeHandlerTestUtil {

    public static final Text edgeTableName = new Text(TableName.EDGE);
    public static final String NB = "\u0000";

    public static ListMultimap<String,String[]> edgeKeyResults = ArrayListMultimap.create();
    public static ListMultimap<String,String> edgeValueResults = ArrayListMultimap.create();

    private static Logger log = Logger.getLogger(EdgeHandlerTestUtil.class);

    public static boolean isDocumentKey(Key k) {
        return isShardKey(k) && k.getColumnFamily().toString().equals(ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY);
    }

    public static boolean isShardKey(Key k) {
        return k.getRow().toString().matches("\\d{8}_\\d+");
    }

    public static void processEvent(Multimap<String,NormalizedContentInterface> eventFields, ExtendedDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler,
                    RawRecordContainer event, int expectedEdgeKeys, boolean printKeysOnlyOnFail, boolean edgeDeleteMode) {

        Assert.assertNotNull("Event was null.", event);
        Set<Key> edgeKeys = new HashSet<>();
        Map<Text,Integer> countMap = Maps.newHashMap();

        // Process edges
        countMap.put(edgeTableName, 0);
        if (null != edgeHandler) {
            EdgeHandlerTestUtil.MyCachingContextWriter contextWriter = new EdgeHandlerTestUtil.MyCachingContextWriter();
            StandaloneTaskAttemptContext<Text,RawRecordContainerImpl,BulkIngestKey,Value> ctx = new StandaloneTaskAttemptContext<>(
                            ((RawRecordContainerImpl) event).getConf(), new StandaloneStatusReporter());

            try {
                contextWriter.setup(ctx.getConfiguration(), false);
                edgeHandler.process(null, event, eventFields, ctx, contextWriter);
                contextWriter.commit(ctx);
                for (Map.Entry<BulkIngestKey,Value> entry : contextWriter.getCache().entries()) {
                    if (entry.getKey().getTableName().equals(edgeTableName)) {
                        edgeKeys.add(entry.getKey().getKey());
                        edgeValueResults.put(entry.getKey().getKey().getRow().toString().replaceAll(NB, "%00;"), EdgeValue.decode(entry.getValue()).toString());
                    }
                    if (!entry.getKey().getTableName().equals(edgeTableName) || entry.getKey().getKey().isDeleted() == edgeDeleteMode) {
                        if (countMap.containsKey(entry.getKey().getTableName())) {
                            countMap.put(entry.getKey().getTableName(), countMap.get(entry.getKey().getTableName()) + 1);
                        } else {
                            countMap.put(entry.getKey().getTableName(), 1);
                        }
                    }
                }
            } catch (Throwable t) {
                log.error("Error during edge processing", t);
                throw new RuntimeException(t);
            }
        }

        Set<String> keyPrint = new TreeSet<>();

        // check edge keys
        for (Key k : edgeKeys) {
            String[] tempArr = {k.getColumnFamily().toString().replaceAll(NB, "%00;"), k.getColumnQualifier().toString().replaceAll(NB, "%00;"),
                    k.getColumnVisibility().toString(), String.valueOf(k.getTimestamp())};
            edgeKeyResults.put(k.getRow().toString().replaceAll(NB, "%00;"), tempArr);

            keyPrint.add("edge key: " + k.getRow().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp()
                            + " ::: " + k.isDeleted() + "\n");
        }

        try {
            if (!printKeysOnlyOnFail) {
                for (String keyString : keyPrint) {
                    log.info(keyString.trim());
                }
            }
            Assert.assertEquals((int) countMap.get(edgeTableName), expectedEdgeKeys);
        } catch (AssertionError ae) {
            if (printKeysOnlyOnFail) {
                for (String keyString : keyPrint) {
                    log.info(keyString.trim());
                }
            }
            final Text shardTableName = new Text(TableName.SHARD);
            Assert.fail(String.format("Expected: %s edge keys.\nFound: %s", expectedEdgeKeys, countMap.get(shardTableName), countMap.get(edgeTableName)));
        }
    }

    private static class MyCachingContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
        private Multimap<BulkIngestKey,Value> cache = HashMultimap.create();

        @Override
        protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,BulkIngestKey,Value> context)
                        throws IOException, InterruptedException {
            for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
                cache.put(entry.getKey(), entry.getValue());
            }
        }

        public Multimap<BulkIngestKey,Value> getCache() {
            return cache;
        }
    }
}
