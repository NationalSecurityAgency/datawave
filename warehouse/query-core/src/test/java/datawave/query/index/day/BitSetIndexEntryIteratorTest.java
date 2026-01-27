package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.util.TableName;

/**
 * Day Index key structure
 * <p>
 *
 * <pre>
 * date value:FIELD-null-datatype bitset
 * </pre>
 */
public class BitSetIndexEntryIteratorTest {

    private static final Logger log = LoggerFactory.getLogger(BitSetIndexEntryIteratorTest.class);

    private static final String DAY_INDEX_TABLE = TableName.SHARD_DAY_INDEX;

    private static DayIndexIngestUtil ingestUtil;

    private final BitSetIndexEntrySerializer serDe = new BitSetIndexEntrySerializer();

    private Multimap<String,String> valuesAndFields;
    private Map<String,BitSet> expected;

    @BeforeAll
    public static void setup() throws Exception {
        // should port over to MiniAccumuloCluster at some point
        InMemoryInstance instance = new InMemoryInstance(BitSetIndexEntryIteratorTest.class.getName());
        ingestUtil = new DayIndexIngestUtil(instance);
        ingestUtil.writeData();
    }

    @BeforeEach
    public void before() {
        valuesAndFields = HashMultimap.create();
        expected = new HashMap<>();
    }

    @Test
    public void testScan_singleDay_SingleField_SingleValue() {
        addTerm("FIELD_A", "even");
        addExpected("20240101", "FIELD_A", "even", ingestUtil.getEven());
        test("20240101");
    }

    @Test
    public void testScan_singleDay_SingleField_MultiValue() {
        addTerm("FIELD_A", "even");
        addTerm("FIELD_A", "odd");
        addExpected("20240101", "FIELD_A", "even", ingestUtil.getEven());
        addExpected("20240101", "FIELD_A", "odd", ingestUtil.getOdd());
        test("20240101");
    }

    @Test
    public void testScan_singleDay_MultiField_SingleValue() {
        addTerm("FIELD_A", "even");
        addTerm("FIELD_B", "even");
        addExpected("20240101", "FIELD_A", "even", ingestUtil.getEven());
        addExpected("20240101", "FIELD_B", "even", ingestUtil.getEven());
        test("20240101");
    }

    @Test
    public void testScan_singleDay_MultiField_MultiValue() {
        addTerm("FIELD_A", "even");
        addTerm("FIELD_A", "odd");
        addTerm("FIELD_B", "even");
        addTerm("FIELD_B", "prime");
        addExpected("20240101", "FIELD_A", "even", ingestUtil.getEven());
        addExpected("20240101", "FIELD_A", "odd", ingestUtil.getOdd());
        addExpected("20240101", "FIELD_B", "even", ingestUtil.getEven());
        addExpected("20240101", "FIELD_B", "prime", ingestUtil.getPrime());
        test("20240101");
    }

    @Test
    public void testScan_singleDay_MissingFieldsAndValues() {
        addTerm("FIELD_A", "hyper-plane"); // valid field, missing value
        addTerm("FIELD_Z", "even"); // missing field, valid value
        test("20240101");
    }

    private void test(String day) {

        assertNotNull(day);
        assertNotNull(valuesAndFields);

        try (Scanner scanner = ingestUtil.getClient().createScanner(DAY_INDEX_TABLE)) {
            scanner.setRange(Range.exact(day));
            IteratorSetting setting = new IteratorSetting(25, "DayEntryIterator", DayIndexEntryIterator.class);
            setting.addOption(DayIndexEntryIterator.VALUES_AND_FIELDS, DayIndexEntryIterator.mapToString(valuesAndFields));
            scanner.addScanIterator(setting);

            for (String field : valuesAndFields.values()) {
                scanner.fetchColumnFamily(new Text(field));
            }

            List<BitSetIndexEntry> results = new LinkedList<>();

            for (Map.Entry<Key,Value> entry : scanner) {
                BitSetIndexEntry result = serDe.deserialize(entry.getValue().get());
                results.add(result);
            }

            assertResults(results);

        } catch (Exception e) {
            fail("Failed with exception", e);
        }
    }

    private void assertResults(List<BitSetIndexEntry> results) {
        for (BitSetIndexEntry result : results) {
            Map<String,BitSet> shards = result.getEntries();
            for (String key : shards.keySet()) {
                String compositeKey = result.getYearOrDay() + " " + key;
                assertTrue(expected.containsKey(compositeKey), "did not find expected key: " + compositeKey);
                BitSet expectedBitSet = expected.remove(compositeKey);
                assertEquals(expectedBitSet, shards.get(key));
            }
        }

        if (!expected.isEmpty()) {
            log.warn("expected results were not found");
            for (String key : expected.keySet()) {
                log.warn(key);
            }
            fail("Expected results not found");
        }
    }

    private void addTerm(String field, String value) {
        valuesAndFields.put(value, field);
    }

    private void addExpected(String day, String field, String value, BitSet bits) {
        String key = day + " " + field + " == '" + value + "'";
        expected.put(key, bits);
    }

}
