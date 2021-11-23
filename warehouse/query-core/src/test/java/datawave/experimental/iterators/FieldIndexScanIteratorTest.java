package datawave.experimental.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.TreeMultimap;

import datawave.experimental.util.AccumuloUtil;
import datawave.query.data.parsers.FieldIndexKey;
import datawave.util.TableName;

class FieldIndexScanIteratorTest {

    protected static AccumuloUtil util;

    @BeforeAll
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(FieldIndexScanIteratorTest.class.getSimpleName());
        util.loadData();
    }

    @Test
    void testSingleTermScan() {
        String query = "FIRST_NAME == 'eve'";
        TreeMultimap<String,String> fieldValues = TreeMultimap.create();
        fieldValues.put("FIRST_NAME", "eve");
        test(fieldValues, new TreeSet<>(util.getEveUids()));
    }

    @Test
    void testSingleFieldMultiValueScan() {
        String query = "FIRST_NAME == 'eve' && FIRST_NAME == 'oberon'";
        TreeMultimap<String,String> fieldValues = TreeMultimap.create();
        fieldValues.putAll("FIRST_NAME", List.of("eve", "oberon"));
        SortedSet<String> expectedUids = new TreeSet<>();
        expectedUids.addAll(util.getEveUids());
        expectedUids.addAll(util.getOberonUids());
        test(fieldValues, expectedUids);
    }

    @Test
    void testMultiFieldMultiValueScan() {
        String query = "(FIRST_NAME == 'oberon' || FIRST_NAME == 'eve' || MSG == 'message'";
        TreeMultimap<String,String> fieldValues = TreeMultimap.create();
        fieldValues.putAll("FIRST_NAME", List.of("oberon", "eve"));
        fieldValues.put("MSG", "message");
        SortedSet<String> expectedUids = new TreeSet<>();
        expectedUids.addAll(util.getEveUids());
        expectedUids.addAll(util.getOberonUids());
        expectedUids.add("sy36z5.-jcat2w.-tgaxdo"); // MSG
        test(fieldValues, expectedUids);
    }

    @Test
    void testFieldValueSerDe() {
        TreeMultimap<String,String> map = TreeMultimap.create();
        map.putAll("FIRST_NAME", List.of("alice", "bob"));

        String ser = FieldIndexScanIterator.serializeFieldValue(map);
        assertEquals("FIRST_NAME:alice,bob", ser);

        TreeMultimap<String,String> deser = FieldIndexScanIterator.deserializeFieldValues(ser);
        assertEquals(map, deser);
    }

    @Test
    void testMultiFieldValueSerDe() {
        TreeMultimap<String,String> map = TreeMultimap.create();
        map.putAll("FIRST_NAME", List.of("alice", "bob"));
        map.putAll("COLOR", List.of("red", "blue"));

        String ser = FieldIndexScanIterator.serializeFieldValue(map);
        assertEquals("COLOR:blue,red;FIRST_NAME:alice,bob", ser);

        TreeMultimap<String,String> deser = FieldIndexScanIterator.deserializeFieldValues(ser);
        assertEquals(map, deser);
    }

    private void test(TreeMultimap<String,String> fieldValues, SortedSet<String> expectedUids) {
        try {
            IteratorSetting setting = new IteratorSetting(100, FieldIndexScanIterator.class);
            setting.addOption(FieldIndexScanIterator.FIELD_VALUES, FieldIndexScanIterator.serializeFieldValue(fieldValues));

            AccumuloClient client = util.getClient();
            long start = System.currentTimeMillis();
            try (Scanner scanner = client.createScanner(TableName.SHARD, util.getAuths())) {
                scanner.addScanIterator(setting);
                scanner.setRange(createRange());

                for (String field : fieldValues.keySet()) {
                    scanner.fetchColumnFamily(new Text("fi\0" + field));
                }

                SortedSet<String> uids = new TreeSet<>();
                FieldIndexKey parser = new FieldIndexKey();
                for (Map.Entry<Key,Value> entry : scanner) {
                    parser.parse(entry.getKey());
                    uids.add(parser.getUid());
                }
                long elapsed = System.currentTimeMillis() - start;
                System.out.println("elapsed: " + elapsed);
                assertEquals(expectedUids, uids);
            }

        } catch (TableNotFoundException e) {
            e.printStackTrace();
            fail("Failed to create scanner");
        }
    }

    private Range createRange() {
        Key start = new Key("20201212_0");
        Key end = start.followingKey(PartialKey.ROW);
        return new Range(start, true, end, false);
    }

}
