package datawave.test.framework.writers;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.RI;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.aggregator.GlobalIndexUidAggregator;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.UidGenerator;

class ShardReverseIndexTableWriterTest extends AbstractTableWriterTest {

    @Test
    void testOnlyReverseIndexedFieldsAreWritten() {
        FieldMetadata forward = createPopulatedField("FORWARD", List.of("Alpha"), List.of(1), I);
        FieldMetadata event = createPopulatedField("EVENT", List.of("Alpha"), List.of(1), E);
        ShardReverseIndexTableWriter.write(client, List.of(forward, event), NUM_SHARDS);

        assertTrue(scanKeys(TableName.SHARD_RINDEX).isEmpty());
    }

    @Test
    void testNormalizeBeforeReversingAndKeepShardDatatypeQualifier() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("École"), List.of(1, 2), RI);
        field.setDatatypes(List.of("dt-a", "dt-b"));
        ShardReverseIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        //  @formatter:off
        assertEquals(List.of(
                "eloce FIELD:20260202_1\0dt-a",
                "eloce FIELD:20260202_1\0dt-b",
                "eloce FIELD:20260202_2\0dt-a",
                "eloce FIELD:20260202_2\0dt-b"), scanKeys(TableName.SHARD_RINDEX));
        //  @formatter:on
        assertTrue(scanKeys(TableName.SHARD_INDEX).isEmpty(), "RI alone must not populate the forward index");
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 20, 21, 25})
    void testRepeatedTokensAndNormalizersCountEachEventOnce(int eventCount) throws InvalidProtocolBufferException {
        List<Integer> eventIds = eventIdsInSameShard(eventCount);
        FieldMetadata field = createPopulatedField("FIELD", List.of("alpha beta alpha"), eventIds, RI, TF);
        field.setNormalizers(normalizers(new LcNoDiacriticsType(), new NoOpType(), new LcNoDiacriticsType()));
        ShardReverseIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        assertEquals(List.of("ahpla FIELD:20260202_1\0" + DATATYPE, "ahpla ateb ahpla FIELD:20260202_1\0" + DATATYPE, "ateb FIELD:20260202_1\0" + DATATYPE),
                        scanKeys(TableName.SHARD_RINDEX));
        for (Map.Entry<Key,Value> entry : scan(TableName.SHARD_RINDEX)) {
            Uid.List uids = Uid.List.parseFrom(entry.getValue().get());
            assertEquals(eventCount, uids.getCOUNT());
            if (eventCount > GlobalIndexUidAggregator.MAX) {
                assertTrue(uids.getIGNORE());
                assertEquals(0, uids.getUIDCount());
            } else {
                assertFalse(uids.getIGNORE());
                List<String> expected = new ArrayList<>();
                for (int eventId : eventIds) {
                    expected.add(UidGenerator.uid(String.valueOf(eventId)));
                }
                List<String> actual = new ArrayList<>(uids.getUIDList());
                expected.sort(String::compareTo);
                actual.sort(String::compareTo);
                assertEquals(expected, actual);
            }
        }
    }

    @Test
    void testDistinctNormalizersProduceDistinctReversedTerms() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha"), List.of(1), RI);
        field.setNormalizers(normalizers(new NoOpType(), new LcNoDiacriticsType()));
        ShardReverseIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        assertEquals(List.of("ahplA FIELD:20260202_1\0" + DATATYPE, "ahpla FIELD:20260202_1\0" + DATATYPE), scanKeys(TableName.SHARD_RINDEX));
    }

    @Test
    void testValueWithNoBackingEventsIsSkipped() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha", "Beta"), List.of(1), RI);
        ShardReverseIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        assertEquals(List.of("ahpla FIELD:20260202_1\0" + DATATYPE), scanKeys(TableName.SHARD_RINDEX));
    }

    @Test
    void testAggregatorIsConfiguredOnEveryScope() throws Exception {
        Map<String,String> properties = new HashMap<>();
        for (Map.Entry<String,String> entry : client.tableOperations().getProperties(TableName.SHARD_RINDEX)) {
            properties.put(entry.getKey(), entry.getValue());
        }
        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            String iterator = "table.iterator." + scope.name() + ".UIDAggregator";
            assertEquals("19,datawave.iterators.TotalAggregatingIterator", properties.get(iterator));
            assertEquals("datawave.ingest.table.aggregator.GlobalIndexUidAggregator", properties.get(iterator + ".opt.*"));
        }
    }

    @Test
    void testEntriesShareTimestampAndVisibility() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha"), List.of(1), RI);
        ShardReverseIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD_RINDEX);
        assertEquals(1, entries.size());
        assertEquals(TableWriter.TIMESTAMP, entries.get(0).getKey().getTimestamp());
        assertEquals(TableWriter.DEFAULT_VISIBILITY, entries.get(0).getKey().getColumnVisibility().toString());
    }
}
