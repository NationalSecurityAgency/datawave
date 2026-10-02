package datawave.test.framework.writers;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.RI;
import static datawave.test.framework.util.MetadataColumn.T;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.ingest.protobuf.TermWeight;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.UidGenerator;

class ShardTableWriterTest extends AbstractTableWriterTest {

    private static final String UID_1 = UidGenerator.uid("1");
    private static final String UID_2 = UidGenerator.uid("2");

    /**
     * The field index key is {@code fi\0FIELD : normalizedValue\0datatype\0uid}, written into the event's shard row.
     */
    @Test
    void testFieldIndexColumn() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha"), List.of(1), I);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        assertEquals(List.of("20260202_1 fi\0FIELD:alpha\0" + DATATYPE + "\0" + UID_1), scanKeys(TableName.SHARD));
    }

    /**
     * The event key is {@code datatype\0uid : FIELD\0value}, and the value is the <b>original</b>, un-normalized text - the event column is what a query
     * returns to the user, so normalizing it here would silently rewrite the document.
     */
    @Test
    void testEventColumnRetainsTheUnnormalizedValue() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha"), List.of(1), E);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        assertEquals(List.of("20260202_1 " + DATATYPE + "\0" + UID_1 + ":FIELD\0Alpha"), scanKeys(TableName.SHARD));
    }

    /**
     * An indexed, event-bearing field writes both the normalized field index entry and the un-normalized event entry for the same event.
     */
    @Test
    void testIndexedEventFieldWritesBothNormalizedAndOriginalValue() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha"), List.of(1), I, E);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        //  @formatter:off
        List<String> expected = List.of(
                "20260202_1 " + DATATYPE + "\0" + UID_1 + ":FIELD\0Alpha",
                "20260202_1 fi\0FIELD:alpha\0" + DATATYPE + "\0" + UID_1);
        //  @formatter:on
        assertEquals(expected, scanKeys(TableName.SHARD));
    }

    /**
     * The term frequency key is {@code tf : datatype\0uid\0normalizedToken\0FIELD}, and its value carries the token's position in the phrase.
     */
    @Test
    void testTermFrequencyColumnRecordsTokenOffsets() throws InvalidProtocolBufferException {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha Beta"), List.of(1), TF);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        //  @formatter:off
        List<String> expected = List.of(
                "20260202_1 tf:" + DATATYPE + "\0" + UID_1 + "\0alpha\0FIELD",
                "20260202_1 tf:" + DATATYPE + "\0" + UID_1 + "\0beta\0FIELD");
        //  @formatter:on
        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD);
        assertEquals(expected, scanKeys(TableName.SHARD));

        assertEquals(List.of(0), offsets(entries.get(0).getValue()), "alpha is the first token");
        assertEquals(List.of(1), offsets(entries.get(1).getValue()), "beta is the second token");
    }

    @Test
    void testRepeatedNormalizedTokensRetainEveryOffset() throws InvalidProtocolBufferException {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha Beta ALPHA"), List.of(1, 11), TF);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD);
        assertEquals(4, entries.size(), "each event has two distinct normalized tokens");
        for (Map.Entry<Key,Value> entry : entries) {
            String qualifier = entry.getKey().getColumnQualifier().toString();
            if (qualifier.endsWith("\0alpha\0FIELD")) {
                assertEquals(List.of(0, 2), offsets(entry.getValue()), "both occurrences belong to the same term frequency entry");
            } else {
                assertEquals(List.of(1), offsets(entry.getValue()));
            }
        }
    }

    @Test
    void testEveryNormalizerContributesOffsetsWithoutDuplicatingPositions() throws InvalidProtocolBufferException {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha alpha ALPHA"), List.of(1), TF);
        field.setNormalizers(normalizers(new NoOpType(), new LcNoDiacriticsType(), new LcNoDiacriticsType()));
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        Map<String,List<Integer>> actual = new HashMap<>();
        for (Map.Entry<Key,Value> entry : scan(TableName.SHARD)) {
            String token = entry.getKey().getColumnQualifier().toString().split("\0")[2];
            actual.put(token, offsets(entry.getValue()));
        }
        assertEquals(Map.of("Alpha", List.of(0), "alpha", List.of(0, 1, 2), "ALPHA", List.of(2)), actual);
    }

    /**
     * A content field indexes the whole phrase and each of its tokens into the field index under one field name.
     */
    @Test
    void testContentFieldIndexesPhraseAndEachToken() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha Beta"), List.of(1), I, TF);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        //  @formatter:off
        List<String> expected = List.of(
                "20260202_1 fi\0FIELD:alpha\0" + DATATYPE + "\0" + UID_1,
                "20260202_1 fi\0FIELD:alpha beta\0" + DATATYPE + "\0" + UID_1,
                "20260202_1 fi\0FIELD:beta\0" + DATATYPE + "\0" + UID_1,
                "20260202_1 tf:" + DATATYPE + "\0" + UID_1 + "\0alpha\0FIELD",
                "20260202_1 tf:" + DATATYPE + "\0" + UID_1 + "\0beta\0FIELD");
        //  @formatter:on
        assertEquals(expected, scanKeys(TableName.SHARD));
    }

    /**
     * The reverse index and type columns describe the metadata table, not the shard table, so they contribute nothing here.
     */
    @Test
    void testReverseIndexAndTypeColumnsAreNoOps() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("a"), List.of(1), RI, T);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        assertTrue(scanKeys(TableName.SHARD).isEmpty(), "RI and T write nothing to the shard table");
    }

    /**
     * Events are grouped into the shard row derived from their event id, which is what keeps a query's ranges bounded.
     */
    @Test
    void testEventsAreGroupedIntoTheirShardRow() {
        // events 1 and 11 share shard 1; event 2 lands in shard 2
        FieldMetadata field = createPopulatedField("FIELD", List.of("a"), List.of(1, 2, 11), E);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        // asserted on the rows alone: the uids are hashes, so their relative order within a shard carries no meaning
        List<String> rows = new ArrayList<>();
        for (Map.Entry<Key,Value> entry : scan(TableName.SHARD)) {
            rows.add(entry.getKey().getRow().toString());
        }
        assertEquals(List.of("20260202_1", "20260202_1", "20260202_2"), rows);

        Set<String> uids = new HashSet<>();
        for (Map.Entry<Key,Value> entry : scan(TableName.SHARD)) {
            uids.add(entry.getKey().getColumnFamily().toString());
        }
        assertEquals(Set.of(DATATYPE + "\0" + UID_1, DATATYPE + "\0" + UID_2, DATATYPE + "\0" + UidGenerator.uid("11")), uids);
    }

    /**
     * Values cycle across the events a field appears on, so consecutive events carry different values.
     */
    @Test
    void testValuesCycleAcrossEvents() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("a", "b"), List.of(1, 2), E);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        //  @formatter:off
        List<String> expected = List.of(
                "20260202_1 " + DATATYPE + "\0" + UID_1 + ":FIELD\0a",
                "20260202_2 " + DATATYPE + "\0" + UID_2 + ":FIELD\0b");
        //  @formatter:on
        assertEquals(expected, scanKeys(TableName.SHARD));
    }

    /**
     * Every entry carries the framework's single timestamp and visibility.
     */
    @Test
    void testEntriesShareTimestampAndVisibility() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("a"), List.of(1), I, E);
        ShardTableWriter.write(client, List.of(field), NUM_SHARDS);

        List<Map.Entry<Key,Value>> entries = scan(TableName.SHARD);
        assertEquals(2, entries.size());
        for (Map.Entry<Key,Value> entry : entries) {
            assertEquals(TableWriter.TIMESTAMP, entry.getKey().getTimestamp());
            assertEquals(TableWriter.DEFAULT_VISIBILITY, entry.getKey().getColumnVisibility().toString());
        }
    }

    private List<Integer> offsets(Value value) throws InvalidProtocolBufferException {
        return TermWeight.Info.parseFrom(value.get()).getTermOffsetList();
    }
}
