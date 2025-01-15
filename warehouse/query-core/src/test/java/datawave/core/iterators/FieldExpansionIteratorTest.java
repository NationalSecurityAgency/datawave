package datawave.core.iterators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.util.TableName;

public class FieldExpansionIteratorTest {

    private static AccumuloClient client;
    private static final Value value = new Value();

    private String startDate;
    private String endDate;
    private final Set<String> fields = new HashSet<>();
    private final Set<String> datatypes = new HashSet<>();
    private final Set<String> expected = new TreeSet<>();

    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        client = new InMemoryAccumuloClient("user", instance);
        client.tableOperations().create(TableName.SHARD_INDEX);
        writeData();
    }

    private static void writeData() throws Exception {
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX)) {
            Mutation m = new Mutation("value");
            // FIELD_A is present in all three days across most datatypes
            m.put("FIELD_A", "20241021_0\u0000datatype-a", value);
            m.put("FIELD_A", "20241022_0\u0000datatype-a", value);
            m.put("FIELD_A", "20241022_0\u0000datatype-b", value);
            m.put("FIELD_A", "20241022_0\u0000datatype-c", value);
            m.put("FIELD_A", "20241023_0\u0000datatype-a", value);
            m.put("FIELD_A", "20241023_0\u0000datatype-b", value);

            // FIELD_B is present in first two days for most datatypes
            m.put("FIELD_B", "20241021_0\u0000datatype-a", value);
            m.put("FIELD_B", "20241022_0\u0000datatype-a", value);
            m.put("FIELD_B", "20241022_0\u0000datatype-b", value);
            m.put("FIELD_B", "20241022_0\u0000datatype-c", value);

            // FIELD_C is present in first two days for some datatypes
            m.put("FIELD_C", "20241021_0\u0000datatype-a", value);
            m.put("FIELD_C", "20241022_0\u0000datatype-a", value);
            m.put("FIELD_C", "20241022_0\u0000datatype-b", value);
            m.put("FIELD_C", "20241022_0\u0000datatype-c", value);

            // FIELD_D supports verification of seeking by start date
            for (int i = 10; i < 30; i++) {
                m.put("FIELD_D", "202410" + i + "_0\u0000datatype-a", value);
            }

            // FIELD_E tests a specific datatype case
            m.put("FIELD_E", "20241023_0\u0000datatype-d", value);

            bw.addMutation(m);
        }
    }

    @BeforeEach
    public void beforeEach() {
        startDate = null;
        endDate = null;
        fields.clear();
        datatypes.clear();
        expected.clear();
    }

    @Test
    public void testSingleDay_noDatatypes() throws Exception {
        withDate("20241021", "20241021");
        withExpected(Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D"));
        drive();
    }

    @Test
    public void testSingleDay_withDatatype() throws Exception {
        withDate("20241021", "20241021");
        withDatatypes(Set.of("datatype-a"));
        withExpected(Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D"));
        drive();
    }

    @Test
    public void testSingleDay_exclusiveDatatype() throws Exception {
        withDate("20241021", "20241021");
        withDatatypes(Set.of("datatype-z"));
        drive();
    }

    @Test
    public void testAllDays_noDatatypes() throws Exception {
        withDate("20241021", "20241023");
        withExpected(Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E"));
        drive();
    }

    @Test
    public void testAllDays_allDatatypes() throws Exception {
        withDate("20241021", "20241023");
        withDatatypes(Set.of("datatype-a", "datatype-b", "datatype-c"));
        withExpected(Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D"));
        drive();
    }

    @Test
    public void testAllDays_someDatatypes() throws Exception {
        withDate("20241021", "20241023");
        withDatatypes(Set.of("datatype-b", "datatype-c"));
        withExpected(Set.of("FIELD_A", "FIELD_B", "FIELD_C"));
        drive();
    }

    @Test
    public void testAllDays_exclusiveDatatypes() throws Exception {
        withDate("20241021", "20241023");
        withDatatypes(Set.of("datatype-z"));
        drive();
    }

    @Test
    public void testSingleDay_withFields_noDatatypes() throws Exception {
        withDate("20241023", "20241023");
        withFields(Set.of("FIELD_A"));
        withExpected(Set.of("FIELD_A"));
        drive();
    }

    @Test
    public void testSingleDay_exclusiveFields_noDatatypes() throws Exception {
        withDate("20241023", "20241023");
        withFields(Set.of("FIELD_B")); // FIELD_B doesn't exist in 2024-10-23
        drive();
    }

    @Test
    public void testSingleDay_withFields_withDatatypes() throws Exception {
        withDate("20241023", "20241023");
        withDatatypes(Set.of("datatype-b")); // datatype exists in 2024-10-23
        withFields(Set.of("FIELD_A"));
        withExpected(Set.of("FIELD_A"));
        drive();
    }

    @Test
    public void testSingleDay_withFields_exclusiveDatatypes() throws Exception {
        withDate("20241023", "20241023");
        withDatatypes(Set.of("datatype-c")); // datatype does not exist in 2024-10-23
        withFields(Set.of("FIELD_A"));
        drive();
    }

    @Test
    public void testSingleDayWithDataAcrossLargeDateRage() throws Exception {
        withDate("20241023", "20241023");
        withDatatypes(Set.of("datatype-a"));
        withFields(Set.of("FIELD_A", "FIELD_D"));
        withExpected(Set.of("FIELD_A", "FIELD_D"));
        drive();
    }

    @Test
    public void testDatatypeSortsAfterTopKey() throws Exception {
        withDate("20241023", "20241023");
        withDatatypes(Set.of("datatype-e"));
        drive();
    }

    public void drive() throws Exception {
        Preconditions.checkNotNull(startDate);
        Preconditions.checkNotNull(endDate);

        IteratorSetting setting = new IteratorSetting(25, FieldExpansionIterator.class.getSimpleName(), FieldExpansionIterator.class);
        setting.addOption(FieldExpansionIterator.START_DATE, startDate);
        setting.addOption(FieldExpansionIterator.END_DATE, endDate);

        if (!fields.isEmpty()) {
            setting.addOption(FieldExpansionIterator.FIELDS, Joiner.on(',').join(fields));
        }

        if (!datatypes.isEmpty()) {
            setting.addOption(FieldExpansionIterator.DATATYPES, Joiner.on(',').join(datatypes));
        }

        Set<String> results = new TreeSet<>();
        try (Scanner scanner = client.createScanner(TableName.SHARD_INDEX)) {
            scanner.addScanIterator(setting);

            Key start = new Key("value");
            Range range = new Range(start, true, start.followingKey(PartialKey.ROW), false);
            scanner.setRange(range);

            for (Map.Entry<Key,Value> keyValueEntry : scanner) {
                Key key = keyValueEntry.getKey();
                results.add(key.getColumnFamily().toString());
            }
        }

        assertEquals(expected, results);
    }

    public void withDate(String startDate, String endDate) {
        assertNotNull(startDate);
        assertNotNull(endDate);
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public void withDatatypes(Set<String> datatypes) {
        assertFalse(datatypes.isEmpty());
        this.datatypes.addAll(datatypes);
    }

    public void withFields(Set<String> fields) {
        assertFalse(fields.isEmpty());
        this.fields.addAll(fields);
    }

    public void withExpected(Set<String> expected) {
        assertFalse(expected.isEmpty());
        this.expected.addAll(expected);
    }
}
