package datawave.query.index.lookup;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MockMetadataHelper;
import datawave.util.TableName;
import datawave.util.time.DateHelper;

/**
 * A suite of tests for a truncated index. Keys take the form:
 *
 * <pre>
 *     value FIELD:yyyyMMdd0x00datatype VIZ (bitset)
 * </pre>
 *
 * The test assertions are simpler than other RangeStream-style tests due to the fact that only shard ranges are generated.
 */
public class TruncatedRangeStreamTest {

    private static final Logger log = LoggerFactory.getLogger(TruncatedRangeStreamTest.class);

    private String query;

    private MockMetadataHelper helper;
    private ShardQueryConfiguration config;

    private final List<String> expected = new ArrayList<>();
    private final List<String> results = new ArrayList<>();

    private final InMemoryInstance instance = new InMemoryInstance(TruncatedRangeStreamTest.class.getName());
    private final AccumuloClient client = new InMemoryAccumuloClient("", instance);
    private final Authorizations auths = new Authorizations("VIZ-A", "VIZ-B", "VIZ-C");
    private final String indexTableName = TableName.TRUNCATED_SHARD_INDEX;
    private final Set<String> indexedFields = Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E");

    private final String DEFAULT_FIELD = "FIELD_A";
    private final String DEFAULT_VALUE = "value-a";
    private final String DEFAULT_DATE = "20251010";
    private final String DEFAULT_DATATYPE = "datatype-a";
    private final String DEFAULT_VISIBILITY = "VIZ-A";

    private TruncatedRangeStreamTest() throws AccumuloSecurityException {
        // required for creating instance/client
    }

    @BeforeEach
    public void beforeEach() {
        expected.clear();
        results.clear();

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

        helper = new MockMetadataHelper();
        helper.setIndexedFields(indexedFields);

        config = new ShardQueryConfiguration();
        config.setIndexedFields(indexedFields);
        config.setClient(client);
        config.setTruncatedIndexTableName(indexTableName);
        config.setAuthorizations(Collections.singleton(auths));
        config.setBeginDate(DateHelper.parse(DEFAULT_DATE));
        config.setEndDate(DateHelper.parse(DEFAULT_DATE));
    }

    @Test
    public void testSingleTermSingleDay() {
        write("value-a", "FIELD_A", createValue(0, 1, 2));
        expect("20251010_0", "20251010_1", "20251010_2");
        withQuery("FIELD_A == 'value-a'");
        withDateRange("20251010", "20251010");
        scan();
    }

    @Test
    public void testSingleTermMultiDay() {
        write("value-a", "FIELD_A", "20251010\0datatype-a", createValue(0, 1, 2));
        write("value-a", "FIELD_A", "20251017\0datatype-a", createValue(11, 22));
        expect("20251010_0", "20251010_1", "20251010_2", "20251017_11", "20251017_22");
        withQuery("FIELD_A == 'value-a'");
        withDateRange("20251010", "20251020");
        scan();
    }

    @Test
    public void testFieldsSkipped() {
        write("value-a", "FIELD_A", createValue(1));
        write("value-a", "FIELD_B", createValue(2));
        write("value-a", "FIELD_C", createValue(3));
        expect("20251010_2");
        withQuery("FIELD_B == 'value-a'");
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
        withQuery("FIELD_A == 'value-a'");
        withDateRange("20251011", "20251013");
        scan();
    }

    @Test
    public void testDateRangeWithGapCorrectlyApplied() {
        write("value-a", "FIELD_A", "20251010\0datatype-a", createValue(1));
        write("value-a", "FIELD_A", "20251014\0datatype-a", createValue(5));
        expect("20251010_1", "20251014_5");
        withQuery("FIELD_A == 'value-a'");
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
        withQuery("FIELD_A == 'value-a'");
        withDatatypes(Set.of("datatype-a", "datatype-c", "datatype-e"));
        scan();
    }

    private void withQuery(String query) {
        this.query = query;
    }

    private void withDateRange(String startDate, String endDate) {
        config.setBeginDate(DateHelper.parse(startDate));
        config.setEndDate(DateHelper.parse(endDate));
    }

    private void withDatatypes(Set<String> datatypes) {
        config.setDatatypeFilter(datatypes);
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

    private void scan() {
        ScannerFactory scannerFactory = new ScannerFactory(client);
        try (var stream = new TruncatedRangeStream(config, scannerFactory, helper); var planIter = stream.streamPlans(parse(query))) {

            for (QueryPlan plan : planIter) {
                Collection<Range> ranges = plan.getRanges();
                assertEquals(1, ranges.size());
                Range range = ranges.iterator().next();
                assertEquals(0, range.getStartKey().getColumnFamily().getLength());
                String row = range.getStartKey().getRow().toString();
                results.add(row);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        assertEquals(expected, results);
    }

    private ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void expect(String... shards) {
        this.expected.addAll(List.of(shards));
    }
}
