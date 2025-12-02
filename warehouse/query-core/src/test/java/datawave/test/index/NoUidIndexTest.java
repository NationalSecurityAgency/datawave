package datawave.test.index;

import static datawave.util.TableName.SHARD_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.junit.jupiter.api.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;

/**
 * Prototypical example of how these tests operate. Typical flow is
 * <ul>
 * <li>write data to shard index</li>
 * <li>declare and assert expected keys</li>
 * <li>create new table via clone and compact</li>
 * <li>declare and assert expected keys</li>
 * </ul>
 */
public class NoUidIndexTest extends IndexConversionUtils implements IndexConversionTests {

    @Test
    public void testDuplicateKeysCollapse() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(1));
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
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(1));
        expect(create("value-b", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(1));
        expect(create("value-c", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(1));
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
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(1));
        expect(create("value-a", "FIELD_B", "20250606_1\0datatype-a"), createNoUidValue(1));
        expect(create("value-a", "FIELD_C", "20250606_1\0datatype-a"), createNoUidValue(1));
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
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(1));
        expect(create("value-a", "FIELD_A", "20250607_1\0datatype-a"), createNoUidValue(1));
        expect(create("value-a", "FIELD_A", "20250608_1\0datatype-a"), createNoUidValue(1));
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
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(1));
        expect(create("value-a", "FIELD_A", "20250606_2\0datatype-a"), createNoUidValue(1));
        expect(create("value-a", "FIELD_A", "20250606_3\0datatype-a"), createNoUidValue(1));
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
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(1));
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-b"), createNoUidValue(1));
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-c"), createNoUidValue(1));
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
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-A"), createNoUidValue(1));
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-B"), createNoUidValue(1));
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a", "VIZ-C"), createNoUidValue(1));
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
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(3));
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
                                }
                            }
                        }
                    }
                }
            }
        }

        cloneAndCompactMac();
        cloneAndCopyIma();

        // this test case only counts the results, perhaps sample a few keys to verify?
        ScanResult macResult = scanMac(getCloneTableName());
        assertEquals(729, macResult.numKeys);

        ScanResult imaResult = scanIma(getCloneTableName());
        assertEquals(729, imaResult.numKeys);
    }

    @Test
    public void testMixOfKeyStructures() {
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createUidValue("uid-a"));
        write(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(23));

        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(24));
        assertUidTable(SHARD_INDEX);

        cloneAndCompactMac();
        cloneAndCopyIma();

        expected.clear();
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(24));
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
        expect(create("value-a", "FIELD_A", "20250606_1\0datatype-a"), createNoUidValue(1));
        assertClonedResults();
    }

    @Override
    protected String getCloneTableName() {
        return "shardIndexNoUids";
    }

    @Override
    protected void configureClonedTable(TableOperations tops, String tableName) {
        try {
            for (var scope : IteratorUtil.IteratorScope.values()) {
                String name = "table.iterator." + scope.name() + ".agg";
                String opt = "table.iterator." + scope.name() + ".agg.opt.*";

                tops.setProperty(getCloneTableName(), name, "19,datawave.iterators.TotalAggregatingIterator");
                tops.setProperty(getCloneTableName(), opt, "datawave.ingest.table.aggregator.KeepCountOnlyNoUidAggregator");
            }
        } catch (Exception e) {
            fail("Failed to configure shard index", e);
        }
    }

    @Override
    protected void assertClonedResults(ScanResult results) {
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).getKey(), results.getResults().get(i).getKey(), "key mismatch");
            try {
                Uid.List expectedList = Uid.List.parseFrom(expected.get(i).getValue().get());
                Uid.List resultList = Uid.List.parseFrom(results.getResults().get(i).getValue().get());

                assertEquals(expectedList.getCOUNT(), resultList.getCOUNT());
                assertEquals(expectedList.getIGNORE(), resultList.getIGNORE());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
