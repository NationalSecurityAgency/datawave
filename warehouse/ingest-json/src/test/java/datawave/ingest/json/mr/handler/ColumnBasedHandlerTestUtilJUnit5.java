package datawave.ingest.json.mr.handler;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.VirtualIngest;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.test.StandaloneStatusReporter;
import datawave.ingest.test.StandaloneTaskAttemptContext;
import datawave.table.constants.ColumnFamilyConstants;
import datawave.table.constants.TableName;

/**
 * Utility Class for common static methods used in ColumnBasedHandler tests
 */
public class ColumnBasedHandlerTestUtilJUnit5 {

    public static final Text shardTableName = new Text(TableName.SHARD);
    public static final Text shardIndexTableName = new Text(TableName.SHARD_INDEX);
    public static final Text shardReverseIndexTableName = new Text(TableName.SHARD_RINDEX);
    public static final Text shardDayIndexTableName = new Text(TableName.SHARD_DAY_INDEX);
    public static final Text edgeTableName = new Text("edge");
    public static final String NULL_BYTE = "\u0000";

    // Shard rows contain an eight-digit date followed by an underscore and a shard number.
    private static final Pattern SHARD_ROW_PATTERN = Pattern.compile("\\d{8}_\\d+");
    private static final String ESCAPED_NULL_BYTE = "%00;";
    private static final String SHARD = "shard";
    private static final String SHARD_INDEX = "shardIndex";
    private static final String REVERSE_SHARD_INDEX = "reverseShardIndex";
    private static final String EDGE = "edge";
    private static final Joiner KEY_ELEMENT_JOINER = Joiner.on(" ::: ");
    private static final Logger log = Logger.getLogger(ColumnBasedHandlerTestUtilJUnit5.class);

    public static boolean isDocumentKey(Key k) {
        return isShardKey(k) && k.getColumnFamily().toString().equals(ColumnFamilyConstants.FULL_CONTENT);
    }

    public static boolean isShardKey(Key k) {
        return SHARD_ROW_PATTERN.matcher(k.getRow().toString()).matches();
    }

    public static InputSplit getSplit(String file) throws URISyntaxException {
        URL data = ColumnBasedHandlerTestUtilJUnit5.class.getResource(file);
        assertNotNull(data, "Did not find test resource");
        File dataFile = new File(data.toURI());
        Path p = new Path(dataFile.toURI().toString());
        return new FileSplit(p, 0, dataFile.length(), null);
    }

    public static void processEvent(DataTypeHandler<Text> handler, RawRecordContainer event, Set<Key> expectedShardKeys, Set<Key> expectedShardIndexKeys,
                    Set<Key> expectedShardReverseIndexKeys) {
        assertNotNull(event, "Event was null.");
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
        Map<Text,Integer> countMap = countByTable(results);

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
                fail("unknown table: " + bik.getTableName() + " key: " + bik.getKey());
            }

        }

        Set<String> errors = new TreeSet<>();

        // Collect all missing and extra keys before failing so the assertion reports every mismatch.
        checkForErrors(expectedShardKeys, shardKeys, SHARD, errors);
        checkForErrors(expectedShardIndexKeys, shardIndexKeys, SHARD_INDEX, errors);
        checkForErrors(expectedShardReverseIndexKeys, shardReverseIndexKeys, REVERSE_SHARD_INDEX, errors);

        for (String error : errors) {
            log.error(error.trim());
        }
        assertTrue(errors.isEmpty(), "Observed errors:\n" + errors);
    }

    public static void processEvent(DataTypeHandler<Text> handler, ExtendedDataTypeHandler<Text,BulkIngestKey,Value> edgeHandler, RawRecordContainer event,
                    int expectedShardKeys, int expectedShardIndexKeys, int expectedShardReverseIndexKeys, int expectedEdgeKeys, boolean printKeysOnlyOnFail) {

        assertNotNull(event, "Event was null.");
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
        Set<Key> shardDayIndexKeys = new HashSet<>();
        Set<Key> edgeKeys = new HashSet<>();
        Map<Text,Integer> countMap = countByTable(results);

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
            } else if (bik.getTableName().equals(shardDayIndexTableName)) {
                shardDayIndexKeys.add(bik.getKey());
            } else {
                fail("unknown table: " + bik.getTableName() + " key: " + bik.getKey());
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
                    incrementTableCount(countMap, entry.getKey().getTableName());
                }
            } catch (Throwable t) {
                log.error("Error during edge processing", t);
                throw new RuntimeException(t);
            }
        }

        Set<String> keyPrint = new TreeSet<>();

        for (Key k : shardKeys) {
            keyPrint.add(formatKey(k, SHARD));
        }

        // check index keys
        for (Key k : shardIndexKeys) {
            keyPrint.add(formatKey(k, SHARD_INDEX));
        }

        // check reverse index keys
        for (Key k : shardReverseIndexKeys) {
            keyPrint.add(formatKey(k, REVERSE_SHARD_INDEX));
        }

        // check edge keys
        for (Key k : edgeKeys) {
            keyPrint.add(formatKey(k, EDGE));
        }

        try {
            if (!printKeysOnlyOnFail) {
                for (String keyString : keyPrint) {
                    log.info(keyString.trim());
                }
            }
            assertTrue(countMap.get(shardTableName) == expectedShardKeys && countMap.get(shardIndexTableName) == expectedShardIndexKeys
                            && countMap.get(shardReverseIndexTableName) == expectedShardReverseIndexKeys && countMap.get(edgeTableName) == expectedEdgeKeys);
        } catch (AssertionError ae) {
            if (printKeysOnlyOnFail) {
                for (String keyString : keyPrint) {
                    log.info(keyString.trim());
                }
            }
            fail(String.format("Expected: %s shard, %s index, %s reverse index, and %s edge keys.\nFound: %s, %s, %s, and %s respectively", expectedShardKeys,
                            expectedShardIndexKeys, expectedShardReverseIndexKeys, expectedEdgeKeys, countMap.get(shardTableName),
                            countMap.get(shardIndexTableName), countMap.get(shardReverseIndexTableName), countMap.get(edgeTableName)));
        }
    }

    static Map<Text,Integer> countByTable(Multimap<BulkIngestKey,Value> results) {
        Map<Text,Integer> countMap = Maps.newHashMap();
        for (BulkIngestKey key : results.keySet()) {
            incrementTableCount(countMap, key.getTableName());
        }
        return countMap;
    }

    private static void incrementTableCount(Map<Text,Integer> countMap, Text tableName) {
        countMap.put(tableName, countMap.getOrDefault(tableName, 0) + 1);
    }

    static void checkForErrors(Set<Key> expected, Set<Key> actual, String descriptor, Set<String> errors) {
        Sets.difference(expected, actual).forEach(key -> errors.add(formatKey(key, "missed " + descriptor)));
        Sets.difference(actual, expected).forEach(key -> errors.add(formatKey(key, "extra " + descriptor)));
    }

    private static String formatKey(Key key, String descriptor) {
        return descriptor + " key: " + KEY_ELEMENT_JOINER.join(replaceNullByte(key.getRow()), replaceNullByte(key.getColumnFamily()),
                        replaceNullByte(key.getColumnQualifier()), key.getColumnVisibility(), key.getTimestamp()) + "\n";
    }

    private static String replaceNullByte(Text text) {
        return text.toString().replace(NULL_BYTE, ESCAPED_NULL_BYTE);
    }

    private static class MyCachingContextWriter extends AbstractContextWriter<BulkIngestKey,Value> {
        private Multimap<BulkIngestKey,Value> cache = HashMultimap.create();

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
}
