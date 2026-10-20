package datawave.ingest.json.mr.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.table.constants.TableName;

class ColumnBasedHandlerTestUtilJUnit5Test {

    @Test
    void reportsMissingAndExtraKeysWithVisibilityTimestampAndNullBytes() {
        String nullByte = ColumnBasedHandlerTestUtilJUnit5.NULL_BYTE;
        Key shared = new Key("shared", "cf", "cq", "PUBLIC", 1L);
        Key missing = new Key("20261019_0", "cf" + nullByte + "part", "cq" + nullByte + "part", "PUBLIC", 123L);
        Key extra = new Key("row" + nullByte + "edge", "cf" + nullByte + "other", "cq", "SECRET", 456L);
        Set<String> errors = new TreeSet<>();

        ColumnBasedHandlerTestUtilJUnit5.checkForErrors(Set.of(shared, missing), Set.of(shared, extra), "edge", errors);

        assertEquals(2, errors.size());
        assertTrue(errors.contains("missed edge key: 20261019_0 ::: cf%00;part ::: cq%00;part ::: PUBLIC ::: 123\n"));
        assertTrue(errors.contains("extra edge key: row%00;edge ::: cf%00;other ::: cq ::: SECRET ::: 456\n"));
    }

    @Test
    void countsDistinctBulkIngestKeysRatherThanValues() {
        Multimap<BulkIngestKey,Value> results = HashMultimap.create();
        BulkIngestKey firstShardKey = new BulkIngestKey(new Text(TableName.SHARD), new Key("row1"));
        BulkIngestKey secondShardKey = new BulkIngestKey(new Text(TableName.SHARD), new Key("row2"));
        BulkIngestKey indexKey = new BulkIngestKey(new Text(TableName.SHARD_INDEX), new Key("row3"));
        results.put(firstShardKey, new Value("first".getBytes(StandardCharsets.UTF_8)));
        results.put(firstShardKey, new Value("second".getBytes(StandardCharsets.UTF_8)));
        results.put(secondShardKey, new Value("third".getBytes(StandardCharsets.UTF_8)));
        results.put(indexKey, new Value("fourth".getBytes(StandardCharsets.UTF_8)));

        Map<Text,Integer> counts = ColumnBasedHandlerTestUtilJUnit5.countByTable(results);

        assertEquals(2, counts.get(new Text(TableName.SHARD)));
        assertEquals(1, counts.get(new Text(TableName.SHARD_INDEX)));
    }

    @Test
    void mapsAllowedTablesAndRetainsDayIndexKeys() {
        Text shardTable = ColumnBasedHandlerTestUtilJUnit5.shardTableName;
        Text dayIndexTable = ColumnBasedHandlerTestUtilJUnit5.shardDayIndexTableName;
        Key shardKey = new Key("20261020_0", "event");
        Key dayIndexKey = new Key("20261020", "field", "value");
        Multimap<BulkIngestKey,Value> results = HashMultimap.create();
        results.put(new BulkIngestKey(shardTable, shardKey), new Value("shard".getBytes(StandardCharsets.UTF_8)));
        results.put(new BulkIngestKey(dayIndexTable, dayIndexKey), new Value("first".getBytes(StandardCharsets.UTF_8)));
        results.put(new BulkIngestKey(dayIndexTable, dayIndexKey), new Value("second".getBytes(StandardCharsets.UTF_8)));

        SetMultimap<Text,Key> tableToKeys = ColumnBasedHandlerTestUtilJUnit5.mapKeysByTable(results, Set.of(shardTable, dayIndexTable));

        assertEquals(Set.of(shardKey), tableToKeys.get(shardTable));
        assertEquals(Set.of(dayIndexKey), tableToKeys.get(dayIndexTable));

        AssertionError error = assertThrows(AssertionError.class, () -> ColumnBasedHandlerTestUtilJUnit5.mapKeysByTable(results, Set.of(shardTable)));
        assertTrue(error.getMessage().contains("unknown table: " + dayIndexTable));
    }
}
