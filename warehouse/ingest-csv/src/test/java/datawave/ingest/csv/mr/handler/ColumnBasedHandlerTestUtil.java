package datawave.ingest.csv.mr.handler;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.ingest.data.config.ingest.VirtualIngest;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.ingest.test.StandaloneTaskAttemptContext;

import datawave.util.TableName;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.junit.Assert;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * Utility Class for common static methods used in ColumnBasedHandler tests
 */
public class ColumnBasedHandlerTestUtil {
    
    public static final Text shardTableName = new Text(TableName.SHARD);
    public static final Text shardIndexTableName = new Text(TableName.SHARD_INDEX);
    public static final Text shardReverseIndexTableName = new Text(TableName.SHARD_RINDEX);
    public static final Text edgeTableName = new Text(TableName.EDGE);
    public static final String NB = "\u0000";
    
    private static Logger log = Logger.getLogger(ColumnBasedHandlerTestUtil.class);
    
    public static boolean isDocumentKey(Key k) {
        return isShardKey(k) && k.getColumnFamily().toString().equals(ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY);
    }
    
    public static boolean isShardKey(Key k) {
        return k.getRow().toString().matches("\\d{8}_\\d+");
    }
    
    public static InputSplit getSplit(String file) throws URISyntaxException {
        URL data = ColumnBasedHandlerTestUtil.class.getResource(file);
        Assert.assertNotNull("Did not find test resource", data);
        File dataFile = new File(data.toURI());
        Path p = new Path(dataFile.toURI().toString());
        return new FileSplit(p, 0, dataFile.length(), null);
    }
    
    public static void processEvent(DataTypeHandler<Text> handler, RawRecordContainer event, Set<Key> expectedShardKeys, Set<Key> expectedShardIndexKeys,
                    Set<Key> expectedShardReverseIndexKeys) {
        Assert.assertNotNull("Event was null.", event);
        Multimap<String,NormalizedContentInterface> eventFields = handler.getHelper(event.getDataType()).getEventFields(event);
        VirtualIngest vHelper = (VirtualIngest) handler.getHelper(event.getDataType());
        Multimap<String,NormalizedContentInterface> virtualFields = vHelper.getVirtualFields(eventFields);
        for (Map.Entry<String,NormalizedContentInterface> v : virtualFields.entries()) {
            eventFields.put(v.getKey(), v.getValue());
        }
        Multimap<BulkIngestKey,Value> results = handler.processBulk(new Text(), event, eventFields, new MockStatusReporter());
        Set<Key> shardKeys = new HashSet<>();
        Set<Key> shardIndexKeys = new HashSet<>();
        Set<Key> shardReverseIndexKeys = new HashSet<>();
        Map<Text,Integer> countMap = Maps.newHashMap();
        for (BulkIngestKey k : results.keySet()) {
            Text tableName = k.getTableName();
            if (countMap.containsKey(tableName)) {
                countMap.put(tableName, countMap.get(tableName) + 1);
            } else {
                countMap.put(tableName, 1);
            }
        }
        
        for (Map.Entry<BulkIngestKey,Value> e : results.entries()) {
            
            BulkIngestKey bik = e.getKey();
            if (log.isDebugEnabled() && isDocumentKey(bik.getKey())) {
                log.debug("Found Document Key: " + bik.getKey());
                log.debug("value:\n" + e.getValue());
            }
            
            if (bik.getTableName().equals(shardTableName)) {
                shardKeys.add(bik.getKey());
            } else if (bik.getTableName().equals(shardIndexTableName)) {
                shardIndexKeys.add(bik.getKey());
            } else if (bik.getTableName().equals(shardReverseIndexTableName)) {
                shardReverseIndexKeys.add(bik.getKey());
            } else {
                Assert.fail("unknown table: " + bik.getTableName() + " key: " + bik.getKey());
            }
            
        }
        
        Set<Key> keys = new HashSet<>();
        Set<String> errors = new TreeSet<>();
        
        /**
         * The following only prints out the missing/extra keys, no test is actually performed until the end. This is done so all errors are known before
         * failing.
         */
        
        // check shard keys
        keys.clear();
        keys.addAll(expectedShardKeys);
        keys.removeAll(shardKeys);
        // check shard keys
        keys.clear();
        keys.addAll(expectedShardKeys);
        keys.removeAll(shardKeys);
        for (Key k : keys) {
            errors.add("missed shard key: " + k.getRow() + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp() + "\n");
        }
        
        keys.addAll(shardKeys);
        keys.removeAll(expectedShardKeys);
        for (Key k : keys) {
            errors.add("extra shard key:  " + k.getRow() + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp() + "\n");
        }
        
        // check index keys
        keys.clear();
        keys.addAll(expectedShardIndexKeys);
        keys.removeAll(shardIndexKeys);
        for (Key k : keys) {
            errors.add("missed shardIndex key: " + k.getRow() + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp() + "\n");
        }
        keys.clear();
        keys.addAll(shardIndexKeys);
        keys.removeAll(expectedShardIndexKeys);
        for (Key k : keys) {
            errors.add("extra shardIndex key:  " + k.getRow() + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp() + "\n");
        }
        
        // check reverse index keys
        keys.clear();
        keys.addAll(expectedShardReverseIndexKeys);
        keys.removeAll(shardReverseIndexKeys);
        for (Key k : keys) {
            errors.add("missed reverseShardIndex key: " + k.getRow() + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp() + "\n");
        }
        keys.clear();
        keys.addAll(shardReverseIndexKeys);
        keys.removeAll(expectedShardReverseIndexKeys);
        for (Key k : keys) {
            errors.add("extra reverseShardIndex key:  " + k.getRow() + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp() + "\n");
        }
        
        for (String error : errors) {
            log.error(error.trim());
        }
        Assert.assertTrue("Observed errors:\n" + errors, errors.isEmpty());
    }
    
    public static void processEvent(DataTypeHandler<Text> handler, ExtendedDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler, RawRecordContainer event,
                    int expectedShardKeys, int expectedShardIndexKeys, int expectedShardReverseIndexKeys, int expectedEdgeKeys, boolean printKeysOnlyOnFail) {
        
        Assert.assertNotNull("Event was null.", event);
        Multimap<String,NormalizedContentInterface> eventFields = handler.getHelper(event.getDataType()).getEventFields(event);
        VirtualIngest vHelper = (VirtualIngest) handler.getHelper(event.getDataType());
        Multimap<String,NormalizedContentInterface> virtualFields = vHelper.getVirtualFields(eventFields);
        for (Map.Entry<String,NormalizedContentInterface> v : virtualFields.entries()) {
            eventFields.put(v.getKey(), v.getValue());
        }
        if (vHelper instanceof CompositeIngest) {
            CompositeIngest compIngest = (CompositeIngest) vHelper;
            Multimap<String,NormalizedContentInterface> compositeFields = compIngest.getCompositeFields(eventFields);
            for (String fieldName : compositeFields.keySet()) {
                // if this is an overloaded event field, we are replacing the existing data
                if (compIngest.isOverloadedCompositeField(fieldName))
                    eventFields.removeAll(fieldName);
                eventFields.putAll(fieldName, compositeFields.get(fieldName));
            }
        }
        Multimap<BulkIngestKey,Value> results = handler.processBulk(new Text(), event, eventFields, new MockStatusReporter());
        Set<Key> shardKeys = new HashSet<>();
        Set<Key> shardIndexKeys = new HashSet<>();
        Set<Key> shardReverseIndexKeys = new HashSet<>();
        Set<Key> edgeKeys = new HashSet<>();
        Map<Text,Integer> countMap = Maps.newHashMap();
        
        for (BulkIngestKey k : results.keySet()) {
            Text tableName = k.getTableName();
            if (countMap.containsKey(tableName)) {
                countMap.put(tableName, countMap.get(tableName) + 1);
            } else {
                countMap.put(tableName, 1);
            }
        }
        
        for (Map.Entry<BulkIngestKey,Value> e : results.entries()) {
            
            BulkIngestKey bik = e.getKey();
            if (log.isDebugEnabled() && isDocumentKey(bik.getKey())) {
                log.debug("Found Document Key: " + bik.getKey());
                log.debug("value:\n" + e.getValue());
            }
            
            if (bik.getTableName().equals(shardTableName)) {
                shardKeys.add(bik.getKey());
            } else if (bik.getTableName().equals(shardIndexTableName)) {
                shardIndexKeys.add(bik.getKey());
            } else if (bik.getTableName().equals(shardReverseIndexTableName)) {
                shardReverseIndexKeys.add(bik.getKey());
            } else {
                Assert.fail("unknown table: " + bik.getTableName() + " key: " + bik.getKey());
            }
            
        }
        
        // Process edges
        countMap.put(edgeTableName, 0);
        if (null != edgeHandler) {
            MyCachingContextWriter contextWriter = new MyCachingContextWriter();
            StandaloneTaskAttemptContext<Text,RawRecordContainerImpl,BulkIngestKey,Value> ctx = new StandaloneTaskAttemptContext<>(
                            ((RawRecordContainerImpl) event).getConf(), new StandaloneStatusReporter());
            
            try {
                contextWriter.setup(ctx.getConfiguration(), false);
                edgeHandler.process(null, event, eventFields, ctx, contextWriter);
                contextWriter.commit(ctx);
                for (Map.Entry<BulkIngestKey,Value> entry : contextWriter.getCache().entries()) {
                    if (entry.getKey().getTableName().equals(edgeTableName)) {
                        edgeKeys.add(entry.getKey().getKey());
                    }
                    if (countMap.containsKey(entry.getKey().getTableName())) {
                        countMap.put(entry.getKey().getTableName(), countMap.get(entry.getKey().getTableName()) + 1);
                    } else {
                        countMap.put(entry.getKey().getTableName(), 1);
                    }
                }
            } catch (Throwable t) {
                log.error("Error during edge processing", t);
                throw new RuntimeException(t);
            }
        }
        
        Set<String> keyPrint = new TreeSet<>();
        
        for (Key k : shardKeys) {
            keyPrint.add("shard key: " + k.getRow() + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp() + "\n");
        }
        
        // check index keys
        for (Key k : shardIndexKeys) {
            keyPrint.add("shardIndex key: " + k.getRow() + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp() + "\n");
        }
        
        // check reverse index keys
        for (Key k : shardReverseIndexKeys) {
            keyPrint.add("reverseShardIndex key: " + k.getRow() + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;") + " ::: "
                            + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: " + k.getTimestamp() + "\n");
        }
        
        // check edge keys
        for (Key k : edgeKeys) {
            keyPrint.add("edge key: " + k.getRow().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnFamily().toString().replaceAll(NB, "%00;")
                            + " ::: " + k.getColumnQualifier().toString().replaceAll(NB, "%00;") + " ::: " + k.getColumnVisibility() + " ::: "
                            + k.getTimestamp() + "\n");
        }
        
        try {
            if (!printKeysOnlyOnFail) {
                for (String keyString : keyPrint) {
                    log.info(keyString.trim());
                }
            }
            if (expectedShardKeys > 0)
                Assert.assertEquals((int) countMap.get(shardTableName), expectedShardKeys);
            if (expectedShardIndexKeys > 0)
                Assert.assertEquals((int) countMap.get(shardIndexTableName), expectedShardIndexKeys);
            if (expectedShardReverseIndexKeys > 0)
                Assert.assertEquals((int) countMap.get(shardReverseIndexTableName), expectedShardReverseIndexKeys);
            if (expectedEdgeKeys > 0)
                Assert.assertEquals((int) countMap.get(edgeTableName), expectedEdgeKeys);
        } catch (AssertionError ae) {
            if (printKeysOnlyOnFail) {
                for (String keyString : keyPrint) {
                    log.info(keyString.trim());
                }
            }
            Assert.fail(String.format("Expected: %s shard, %s index, %s reverse index, and %s edge keys.\nFound: %s, %s, %s, and %s respectively",
                            expectedShardKeys, expectedShardIndexKeys, expectedShardReverseIndexKeys, expectedEdgeKeys, countMap.get(shardTableName),
                            countMap.get(shardIndexTableName), countMap.get(shardReverseIndexTableName), countMap.get(edgeTableName)));
        }
    }
    
    private static class MyCachingContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
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
}
