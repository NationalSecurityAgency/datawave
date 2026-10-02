package datawave.test.framework.writers;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.ShardKeyUtil;
import datawave.util.time.DateHelper;

class TableWriterTest extends AbstractTableWriterTest {

    /**
     * One call populates all three tables, so a test fixture cannot accidentally write the shard table while leaving the metadata a query planner reads out of
     * date.
     */
    @Test
    void testWritesEveryTable() {
        FieldMetadata field = createPopulatedField("FIELD", List.of("Alpha Beta"), List.of(1, 2), I, E, TF);
        TableWriter.write(client, List.of(field), NUM_SHARDS);

        assertFalse(scanKeys(TableName.METADATA).isEmpty(), "metadata table should be populated");
        assertFalse(scanKeys(TableName.SHARD_INDEX).isEmpty(), "shard index should be populated");
        assertFalse(scanKeys(TableName.SHARD).isEmpty(), "shard table should be populated");
    }

    /**
     * Delegation is exact: writing through {@link TableWriter} produces the same entries as calling each writer directly.
     */
    @Test
    void testDelegatesToEachWriterWithoutAlteringOutput() throws Exception {
        FieldMetadata combined = createPopulatedField("FIELD", List.of("Alpha Beta"), List.of(1, 2), I, E, TF);
        TableWriter.write(client, List.of(combined), NUM_SHARDS);

        List<String> metadata = scanKeys(TableName.METADATA);
        List<String> index = scanKeys(TableName.SHARD_INDEX);
        List<String> shard = scanKeys(TableName.SHARD);

        // rebuild against a clean instance, driving the writers individually
        createClientAndTables();
        FieldMetadata direct = createPopulatedField("FIELD", List.of("Alpha Beta"), List.of(1, 2), I, E, TF);
        MetadataTableWriter.write(client, List.of(direct));
        ShardIndexTableWriter.write(client, List.of(direct), NUM_SHARDS);
        ShardTableWriter.write(client, List.of(direct), NUM_SHARDS);

        assertEquals(metadata, scanKeys(TableName.METADATA));
        assertEquals(index, scanKeys(TableName.SHARD_INDEX));
        assertEquals(shard, scanKeys(TableName.SHARD));
    }

    /**
     * Every table is written at the timestamp derived from the shard date, so a query bounded by that date covers all of them.
     */
    @Test
    void testTimestampMatchesTheShardDate() {
        assertEquals(DateHelper.parse(ShardKeyUtil.DATE).getTime(), TableWriter.TIMESTAMP);
    }
}
