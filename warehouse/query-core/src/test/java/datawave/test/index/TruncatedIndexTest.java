package datawave.test.index;

import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.BitSet;
import java.util.List;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.junit.jupiter.api.Test;

public class TruncatedIndexTest extends IndexConversionUtils implements IndexConversionTests {

    @Test
    public void testDuplicateKeysCollapse() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        assertClonedResults();
    }

    @Test
    public void testVariableValues() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-b", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-c", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        expect(create("value-b", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        expect(create("value-c", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        expect(create("value-b", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        expect(create("value-c", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        assertClonedResults();
    }

    @Test
    public void testVariableFields() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_B", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_C", "20250606_1\0datatype-a"), createUidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        expect(create("value-a", "FIELD_B", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        expect(create("value-a", "FIELD_C", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        expect(create("value-a", "FIELD_B", "20250606\0datatype-a"), createBitSetValue(1));
        expect(create("value-a", "FIELD_C", "20250606\0datatype-a"), createBitSetValue(1));
        assertClonedResults();
    }

    @Test
    public void testVariableDays() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250607_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250608_1\0datatype-a"), createUidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        expect(create("value-a", "FIELD_A", "20250607_1\0datatype-a"), createUidValue("uid-a"));
        expect(create("value-a", "FIELD_A", "20250608_1\0datatype-a"), createUidValue("uid-a"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250607\0datatype-a"), createBitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250608\0datatype-a"), createBitSetValue(1));
        assertClonedResults();
    }

    @Test
    public void testVariableShards() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_2\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_3\0datatype-a"), createUidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        expect(create("value-a", "FIELD_A", "20250606_2\0datatype-a"), createUidValue("uid-a"));
        expect(create("value-a", "FIELD_A", "20250606_3\0datatype-a"), createUidValue("uid-a"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1, 2, 3));
        assertClonedResults();
    }

    @Test
    public void testVariableDatatypes() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-b"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-c"), createUidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-b"), createUidValue("uid-a"));
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-c"), createUidValue("uid-a"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-b"), createBitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-c"), createBitSetValue(1));
        assertClonedResults();
    }

    @Test
    public void testVariableVisibilities() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-A"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-B"), createUidValue("uid-b"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-C"), createUidValue("uid-c"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-A"), createUidValue("uid-a"));
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-B"), createUidValue("uid-b"));
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-C"), createUidValue("uid-c"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a", "VIZ-A"), createBitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a", "VIZ-B"), createBitSetValue(1));
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a", "VIZ-C"), createBitSetValue(1));
        assertClonedResults();
    }

    @Test
    public void testVariableUids() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-b"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-c"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a", "uid-b", "uid-c"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        assertClonedResults();
    }

    @Test
    public void testPermutationsAndCompareTables() {
        List<String> values = List.of("value-a", "value-b", "value-c");
        List<String> fields = List.of("FIELD_A", "FIELD_B", "FIELD_C");
        List<String> dates = List.of("20250606", "20250607", "20250608");
        List<String> shards = List.of("_1", "_2", "_3");
        List<String> datatypes = List.of("datatype-a", "datatype-b", "datatype-c");
        List<String> visibilities = List.of("VIZ-A", "VIZ-B", "VIZ-C");
        List<String> uids = List.of("uid-a", "uid-b", "uid-c");

        int written = 0;
        for (String value : values) {
            for (String field : fields) {
                for (String date : dates) {
                    for (String num : shards) {
                        for (String datatype : datatypes) {
                            for (String visibility : visibilities) {
                                for (String uid : uids) {
                                    Key key = create(value, field, date + num + "\0" + datatype, visibility);
                                    Value v = createUidValue(uid);
                                    write(key, v);
                                    written++;
                                }
                            }
                        }
                    }
                }
            }
        }

        assertEquals(2187, written);
        cloneAndCompactMac();
        cloneAndCopyIma();

        // this test case only counts the results, perhaps sample a few keys to verify?
        ScanResult macResult = scanMac(getCloneTableName());
        assertEquals(243, macResult.numKeys);

        ScanResult imaResult = scanIma(getCloneTableName());
        assertEquals(243, imaResult.numKeys);
    }

    @Test
    public void testMixOfKeyStructures() {
        cloneAndCompactMac();
        cloneAndCopyIma();

        write(getCloneTableName(), create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(getCloneTableName(), create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));

        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        assertClonedResults();
    }

    @Test
    public void testDeletes() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(delete("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606\0datatype-a"), createBitSetValue(1));
        assertClonedResults();
    }

    @Override
    protected String getCloneTableName() {
        return "truncatedIndex";
    }

    @Override
    protected void configureClonedTable(TableOperations tops, String tableName) {
        try {
            for (var scope : IteratorUtil.IteratorScope.values()) {
                String name = "table.iterator." + scope.name() + ".agg";
                String opt = "table.iterator." + scope.name() + ".agg.opt.*";
                tops.removeProperty(tableName, name);
                tops.removeProperty(tableName, opt);

                String truncatedName = "table.iterator." + scope.name() + ".truncate";
                tops.setProperty(tableName, truncatedName, "18,datawave.ingest.table.aggregator.TruncatedIndexConversionIterator");

                String combinerName = "table.iterator." + scope.name() + ".bits";
                String combinerOpt = "table.iterator." + scope.name() + ".bits.opt.all";
                tops.setProperty(tableName, combinerName, "19,datawave.ingest.table.aggregator.BitSetCombiner");
                tops.setProperty(tableName, combinerOpt, "true");
            }
        } catch (Exception e) {
            fail("Failed to configure shard index", e);
        }
    }

    @Override
    protected void assertClonedResults(ScanResult results) {
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).getKey(), results.getResults().get(i).getKey(), "key mismatch");

            BitSet expectedBits = BitSet.valueOf(expected.get(i).getValue().get());
            BitSet resultBits = BitSet.valueOf(results.getResults().get(i).getValue().get());
            assertEquals(expectedBits, resultBits, "bitset mismatch");
        }
    }
}
