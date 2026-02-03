package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.util.Tuple2;
import datawave.util.TableName;

public class TruncatedIndexScannerTest {

    private static final Logger log = LoggerFactory.getLogger(TruncatedIndexScannerTest.class);

    private final InMemoryInstance instance = new InMemoryInstance(TruncatedIndexScannerTest.class.getName());
    private final AccumuloClient client = new InMemoryAccumuloClient("", instance);
    private final Authorizations auths = new Authorizations("VIZ-A", "VIZ-B", "VIZ-C");
    private final String indexTableName = TableName.TRUNCATED_SHARD_INDEX;

    private final String DEFAULT_FIELD = "FIELD_A";
    private final String DEFAULT_VALUE = "value-a";
    private final String DEFAULT_DATE = "20251010";
    private final String DEFAULT_DATATYPE = "datatype-a";
    private final String DEFAULT_VISIBILITY = "VIZ-A";

    private String field;
    private String value;
    private String startDate;
    private String endDate;
    private final Set<String> datatypes = new HashSet<>();

    private final List<String> expected = new ArrayList<>();
    private final List<String> results = new ArrayList<>();

    private TruncatedIndexScannerTest() throws AccumuloSecurityException {
        // required for setting up instance and client
    }

    @BeforeEach
    public void beforeEach() {
        expected.clear();
        results.clear();

        field = DEFAULT_FIELD;
        value = DEFAULT_VALUE;
        startDate = DEFAULT_DATE;
        endDate = DEFAULT_DATE;
        datatypes.clear();

        TableOperations tops = client.tableOperations();
        try {
            if (tops.exists(indexTableName)) {
                tops.delete(indexTableName);
            }
            tops.create(indexTableName);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSingleDayScan() {
        write("value-a", "FIELD_A", createValue(1, 2, 3));
        expect("20251010_1", "20251010_2", "20251010_3");
        scan();
    }

    @Test
    public void testMultiDayScan() {
        write("value-a", "FIELD_A", "20251010\0datatype-a", createValue(2, 4));
        write("value-a", "FIELD_A", "20251011\0datatype-a", createValue(7, 9));
        expect("20251010_2", "20251010_4", "20251011_7", "20251011_9");
        withDateRange("20251010", "20251011");
        scan();
    }

    @Test
    public void testFieldsSkipped() {
        write("value-a", "FIELD_A", createValue(1));
        write("value-a", "FIELD_B", createValue(2));
        write("value-a", "FIELD_C", createValue(3));
        expect("20251010_2");
        withFieldValue("FIELD_B", "value-a");
        scan();
    }

    @Test
    public void testDateRangeCorrectlyApplied() {
        write("value-a", "FIELD_A", "20251010\0datatype-a", createValue(1));
        write("value-a", "FIELD_A", "20251011\0datatype-a", createValue(2));
        write("value-a", "FIELD_A", "20251012\0datatype-a", createValue(3));
        write("value-a", "FIELD_A", "20251013\0datatype-a", createValue(4));
        write("value-a", "FIELD_A", "20251014\0datatype-a", createValue(5));
        expect("20251011_2", "20251012_3", "20251013_4");
        withDateRange("20251011", "20251013");
        scan();
    }

    @Test
    public void testDateRangeWithGapCorrectlyApplied() {
        write("value-a", "FIELD_A", "20251010\0datatype-a", createValue(1));
        write("value-a", "FIELD_A", "20251014\0datatype-a", createValue(5));
        expect("20251010_1", "20251014_5");
        withDateRange("20251010", "20251020");
        scan();
    }

    @Test
    public void testDatatypeFilterCorrectlyApplied() {
        write("value-a", "FIELD_A", "20251010\0datatype-a", createValue(1));
        write("value-a", "FIELD_A", "20251010\0datatype-b", createValue(2));
        write("value-a", "FIELD_A", "20251010\0datatype-c", createValue(3));
        write("value-a", "FIELD_A", "20251010\0datatype-d", createValue(4));
        write("value-a", "FIELD_A", "20251010\0datatype-e", createValue(5));
        expect("20251010_1", "20251010_3", "20251010_5");
        withDatatypes(Set.of("datatype-a", "datatype-c", "datatype-e"));
        scan();
    }

    private void withFieldValue(String field, String value) {
        this.field = field;
        this.value = value;
    }

    private void withDateRange(String startDate, String endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }

    private void withDatatypes(Set<String> datatypes) {
        this.datatypes.addAll(datatypes);
    }

    private void write(String row, String cf, Value value) {
        String cq = DEFAULT_DATE + "\0" + DEFAULT_DATATYPE;
        write(row, cf, cq, DEFAULT_VISIBILITY, value);
    }

    private void write(String row, String cf, String cq, Value value) {
        write(row, cf, cq, DEFAULT_VISIBILITY, value);
    }

    private void write(String row, String cf, String cq, String viz, Value value) {
        try (var bw = client.createBatchWriter(TableName.TRUNCATED_SHARD_INDEX)) {
            Mutation m = new Mutation(row);
            ColumnVisibility cv = new ColumnVisibility(viz);
            m.put(cf, cq, cv, value);
            bw.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private Value createValue(int... offsets) {
        BitSet bitset = new BitSet();
        for (int offset : offsets) {
            bitset.set(offset);
        }
        return new Value(bitset.toByteArray());
    }

    private void expect(String... shards) {
        expected.addAll(List.of(shards));
    }

    private void scan() {
        TruncatedIndexScanner scanner = new TruncatedIndexScanner(client, startDate, endDate);
        scanner.setFieldValue(field, value);
        scanner.setAuths(auths);
        scanner.setTableName(indexTableName);
        if (!datatypes.isEmpty()) {
            scanner.setDatatypes(datatypes);
        }
        scanner.setBasePriority(30);

        while (scanner.hasNext()) {
            // only extract the shard, the IndexInfo is always the same
            Tuple2<String,IndexInfo> entry = scanner.next();
            results.add(entry.first());
        }
        assertEquals(expected, results);
    }

}
