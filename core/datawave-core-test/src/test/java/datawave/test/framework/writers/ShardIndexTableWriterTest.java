package datawave.test.framework.writers;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.admin.TableOperations;
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
import datawave.test.framework.TableCreator;
import datawave.test.framework.util.MacTestUtil;
import datawave.test.framework.util.UidGenerator;

class ShardIndexTableWriterTest extends AbstractTableWriterTest {

    /**
     * The global index only describes indexed fields; an event-only field has nothing to look up.
     */
    @Test
    void testOnlyIndexedFieldsAreWritten() {
        FieldMetadata eventOnly = createPopulatedField("EVENT_ONLY", List.of("a"), List.of(1), E);
        ShardIndexTableWriter.write(client, List.of(eventOnly), NUM_SHARDS);

        assertTrue(scanKeys(TableName.SHARD_INDEX).isEmpty(), "an event-only field should not be globally indexed");
    }

    /**
     * The row is the normalized value, the column family is the field name and the qualifier pairs the shard with the datatype.
     */
    @Test
    void testRowIsNormalizedValueAndQualifierPairsShardWithDatatype() {
        // event 1 lands in shard 1 and event 2 in shard 2, so the qualifiers differ
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha"), List.of(1, 2), I);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        //  @formatter:off
        List<String> expected = List.of(
                "alpha FIELD:20260202_1\0" + DATATYPE,
                "alpha FIELD:20260202_2\0" + DATATYPE);
        //  @formatter:on
        assertEquals(expected, scanKeys(TableName.SHARD_INDEX));
    }

    /**
     * A single event produces a {@link Uid.List} of exactly one uid with a count of one. A count of one is correct per entry: the aggregator sums the entries
     * that collide within a shard into the real per-shard count.
     */
    @Test
    void testASingleEventCarriesASingleUidList() throws InvalidProtocolBufferException {
        FieldMetadata field = createPopulatedField("FIELD", List.of("a"), List.of(1), I);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD_INDEX);
        assertEquals(1, entries.size());

        Uid.List uids = Uid.List.parseFrom(entries.get(0).getValue().get());
        assertEquals(1L, uids.getCOUNT());
        assertFalse(uids.getIGNORE());
        assertEquals(List.of(UidGenerator.uid("1")), uids.getUIDList());
    }

    /**
     * Two events sharing a value within one shard produce byte-for-byte identical keys, differing only in their {@link Uid.List} value. They survive as a
     * single multi-uid entry purely because {@link TableCreator#createShardIndex} installs the uid aggregator <b>beneath</b> the versioning iterator: at
     * priority 19 the collisions are summed before versioning can discard them.
     * <p>
     * This is the assertion that fails if that aggregator is ever removed or reprioritised above the versioning iterator, a regression that otherwise surfaces
     * only as query tests quietly returning too few results.
     */
    @Test
    void testAggregatorMergesCollidingUidsWithinAShard() throws InvalidProtocolBufferException {
        // events 1 and 11 both land in shard 1 under numShards 10
        FieldMetadata field = createPopulatedField("FIELD", List.of("a"), List.of(1, 11), I);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD_INDEX);
        assertEquals(1, entries.size(), "both events share a shard, so the aggregator should leave a single entry");
        assertEquals("a FIELD:20260202_1\0" + DATATYPE, scanKeys(TableName.SHARD_INDEX).get(0));

        Uid.List uids = Uid.List.parseFrom(entries.get(0).getValue().get());
        assertEquals(2L, uids.getCOUNT(), "the aggregated count should reflect both events");
        assertFalse(uids.getIGNORE());
        assertEquals(sorted(List.of(UidGenerator.uid("1"), UidGenerator.uid("11"))), sorted(uids.getUIDList()));
    }

    /**
     * Raising the aggregator above the versioning iterator lets versioning discard the colliding entries first, degrading the index to one uid per shard. The
     * priority in {@link TableCreator#createShardIndex} is therefore load-bearing rather than incidental.
     */
    @Test
    void testAggregatorAboveVersioningLosesUids() throws Exception {
        TableOperations tops = client.tableOperations();
        Map<String,String> raised = new HashMap<>();
        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            raised.put("table.iterator." + scope.name() + ".UIDAggregator", "21,datawave.iterators.TotalAggregatingIterator");
        }
        MacTestUtil.addPropertiesAndWait(tops, TableName.SHARD_INDEX, raised);

        FieldMetadata field = createPopulatedField("FIELD", List.of("a"), List.of(1, 11), I);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD_INDEX);
        assertEquals(1, entries.size());

        Uid.List uids = Uid.List.parseFrom(entries.get(0).getValue().get());
        assertEquals(1, uids.getUIDCount(), "above the versioning iterator the aggregator only ever sees one of the colliding entries");
    }

    /**
     * The aggregator is installed on every iterator scope, so a scan, a minor compaction and a major compaction all agree on the uid list.
     */
    @Test
    void testAggregatorIsConfiguredOnEveryScope() throws Exception {
        Map<String,String> properties = new HashMap<>();
        for (Map.Entry<String,String> property : client.tableOperations().getProperties(TableName.SHARD_INDEX)) {
            properties.put(property.getKey(), property.getValue());
        }

        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            String iterator = "table.iterator." + scope.name() + ".UIDAggregator";
            assertEquals("19,datawave.iterators.TotalAggregatingIterator", properties.get(iterator), "aggregator must sit below versioning on " + scope);
            assertEquals("datawave.ingest.table.aggregator.GlobalIndexUidAggregator", properties.get(iterator + ".opt.*"), "wrong aggregator on " + scope);
        }
    }

    /**
     * Events in different shards are different keys, so each keeps its own single-uid list.
     */
    @Test
    void testEventsInDifferentShardsRemainSeparateEntries() throws InvalidProtocolBufferException {
        FieldMetadata field = createPopulatedField("FIELD", List.of("a"), List.of(1, 2), I);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD_INDEX);
        assertEquals(2, entries.size());
        for (Map.Entry<Key,Value> entry : entries) {
            Uid.List uids = Uid.List.parseFrom(entry.getValue().get());
            assertEquals(1L, uids.getCOUNT());
            assertEquals(1, uids.getUIDCount());
        }
    }

    /**
     * A value can outnumber the events a field appears on, leaving it with no backing events. Writing it would produce a mutation with no puts, which Accumulo
     * rejects, so it must be skipped - the value still exists as a valid "matches nothing" query case.
     */
    @Test
    void testValueWithNoBackingEventsIsSkipped() {
        // one event and two values: event 1 maps to "a", leaving "b" with no events
        FieldMetadata field = createPopulatedField("FIELD", List.of("a", "b"), List.of(1), I);
        assertTrue(field.getEventIdsForValue("b").isEmpty(), "fixture expects 'b' to have no backing events");

        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        assertEquals(List.of("a FIELD:20260202_1\0" + DATATYPE), scanKeys(TableName.SHARD_INDEX));
    }

    /**
     * A content field indexes the whole phrase and each of its tokens under the same field name, so {@code content:phrase}'s {@code FIELD == 'word'} index
     * expansion can resolve.
     */
    @Test
    void testContentFieldIndexesPhraseAndEachToken() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha Beta"), List.of(1), I, TF);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        //  @formatter:off
        List<String> expected = List.of(
                "alpha FIELD:20260202_1\0" + DATATYPE,
                "alpha beta FIELD:20260202_1\0" + DATATYPE,
                "beta FIELD:20260202_1\0" + DATATYPE);
        //  @formatter:on
        assertEquals(expected, scanKeys(TableName.SHARD_INDEX));
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 20, 21, 25})
    void testRepeatedNormalizedTokensCountEachEventOnce(int eventCount) throws InvalidProtocolBufferException {
        List<Integer> eventIds = eventIdsInSameShard(eventCount);
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha Beta ALPHA"), eventIds, I, TF);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD_INDEX);
        assertEquals(3, entries.size(), "index the phrase and both distinct tokens");
        for (Map.Entry<Key,Value> entry : entries) {
            Uid.List uids = Uid.List.parseFrom(entry.getValue().get());
            assertCountAndUids(uids, eventIds);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 20, 21, 25})
    void testOverlappingNormalizersCountEachEventOnce(int eventCount) throws InvalidProtocolBufferException {
        List<Integer> eventIds = eventIdsInSameShard(eventCount);
        FieldMetadata field = createPopulatedField("FIELD", List.of("alpha"), eventIds, I, TF);
        field.setNormalizers(normalizers(new LcNoDiacriticsType(), new NoOpType(), new LcNoDiacriticsType()));
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD_INDEX);
        assertEquals(1, entries.size(), "the phrase and token normalize to the same index key");
        Uid.List uids = Uid.List.parseFrom(entries.get(0).getValue().get());
        assertCountAndUids(uids, eventIds);
    }

    @Test
    void testDuplicateSourceValuesRetainCountOnlyEstimation() throws InvalidProtocolBufferException {
        FieldMetadata field = createPopulatedField("FIELD", List.of("a", "a"), eventIdsInSameShard(21), I);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD_INDEX);
        assertEquals(1, entries.size());
        Uid.List uids = Uid.List.parseFrom(entries.get(0).getValue().get());
        assertTrue(uids.getIGNORE());
        assertEquals(0, uids.getUIDCount());
        // Duplicate source values intentionally emit duplicate entries; count-only aggregation estimates their summed count.
        assertEquals(42L, uids.getCOUNT());
    }

    /**
     * A non-content field indexes only its whole value, even when that value contains a space.
     */
    @Test
    void testNonContentFieldDoesNotIndexTokens() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("alpha beta"), List.of(1), I);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        assertEquals(List.of("alpha beta FIELD:20260202_1\0" + DATATYPE), scanKeys(TableName.SHARD_INDEX));
    }

    /**
     * Every entry carries the framework's single timestamp and visibility.
     */
    @Test
    void testEntriesShareTimestampAndVisibility() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("a"), List.of(1), I);
        ShardIndexTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD_INDEX);
        assertEquals(1, entries.size());
        assertEquals(TableWriter.TIMESTAMP, entries.get(0).getKey().getTimestamp());
        assertEquals(TableWriter.DEFAULT_VISIBILITY, entries.get(0).getKey().getColumnVisibility().toString());
    }

    private List<String> sorted(List<String> uids) {
        List<String> copy = new ArrayList<>(uids);
        copy.sort(String::compareTo);
        return copy;
    }

    private void assertCountAndUids(Uid.List uids, List<Integer> eventIds) {
        assertEquals(eventIds.size(), uids.getCOUNT(), "token repetitions and overlapping normalizers must not inflate the event count");
        if (eventIds.size() > GlobalIndexUidAggregator.MAX) {
            assertTrue(uids.getIGNORE(), "above the UID threshold the aggregator must use count-only mode");
            assertEquals(0, uids.getUIDCount());
        } else {
            assertFalse(uids.getIGNORE());
            List<String> expected = new ArrayList<>();
            for (int eventId : eventIds) {
                expected.add(UidGenerator.uid(String.valueOf(eventId)));
            }
            assertEquals(sorted(expected), sorted(uids.getUIDList()));
        }
    }
}
