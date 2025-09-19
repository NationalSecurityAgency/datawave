package datawave.query.jexl.lookups;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.util.TableName;
import datawave.util.time.DateHelper;

/**
 * Collection of helpful methods for testing {@link IndexLookup}s.
 * <p>
 * Note: these tests use an {@link InMemoryInstance} and in some cases it is better to test with {@link MiniAccumuloCluster}
 */
public abstract class BaseIndexLookupTest {

    private static final Logger log = LoggerFactory.getLogger(BaseIndexLookupTest.class);

    private static final String DEFAULT_DATE = "20250606";
    private static final String DEFAULT_DATATYPE = "datatype";
    private static final Value EMPTY_VALUE = new Value();
    protected ExecutorService executor;

    protected static AccumuloClient client;
    protected static TableOperations tops;

    protected final Set<String> indexedFields = Set.of("_ANYFIELD_", "FIELD_A", "FIELD_B", "FIELD_C");

    protected final ShardQueryConfiguration config = new ShardQueryConfiguration();
    protected final MockMetadataHelper metadataHelper = new MockMetadataHelper();

    protected String query = null;
    private IndexLookupMap result;

    private final StringBuilder sb = new StringBuilder();

    @BeforeAll
    public static void setup() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        client = new InMemoryAccumuloClient("root", instance);
        tops = client.tableOperations();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        if (tops.exists(TableName.SHARD_INDEX)) {
            tops.delete(TableName.SHARD_INDEX);
        }
        tops.create(TableName.SHARD_INDEX);

        if (tops.exists(TableName.SHARD_RINDEX)) {
            tops.delete(TableName.SHARD_RINDEX);
        }
        tops.create(TableName.SHARD_RINDEX);

        config.setClient(client);
        config.setBeginDate(DateHelper.parse("20250606"));
        config.setEndDate(DateHelper.parse("20250606"));

        metadataHelper.setIndexedFields(indexedFields);
        metadataHelper.setReverseIndexFields(indexedFields);

        query = null;
        result = null;
        executor = Executors.newFixedThreadPool(5);
    }

    @AfterEach
    public void afterEach() {
        executor.shutdownNow();
    }

    public void write(String value, String field) {
        write(value, field, DEFAULT_DATE, DEFAULT_DATATYPE, EMPTY_VALUE);
    }

    public void write(String value, String field, Value v) {
        write(value, field, DEFAULT_DATE, DEFAULT_DATATYPE, v);
    }

    public void write(String value, String field, String date, String datatype) {
        write(value, field, date, datatype, EMPTY_VALUE);
    }

    public void write(String value, String field, String date, String datatype, Value v) {
        try (var writer = client.createBatchWriter(TableName.SHARD_INDEX)) {
            Mutation m = new Mutation(value);
            m.put(field, date + "\0" + datatype, v);
            writer.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void writeReverse(String value, String field) {
        writeReverse(value, field, DEFAULT_DATE, DEFAULT_DATATYPE, EMPTY_VALUE);
    }

    public void writeReverse(String value, String field, Value v) {
        writeReverse(value, field, DEFAULT_DATE, DEFAULT_DATATYPE, v);
    }

    public void writeReverse(String value, String field, String date, String datatype) {
        writeReverse(value, field, date, datatype, EMPTY_VALUE);
    }

    public void writeReverse(String value, String field, String date, String datatype, Value v) {
        try (var writer = client.createBatchWriter(TableName.SHARD_RINDEX)) {
            Mutation m = new Mutation(reverse(value));
            m.put(field, date + "\0" + datatype, v);
            writer.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            log.error(e.getMessage(), e);
        }
    }

    private String reverse(String value) {
        sb.setLength(0);
        sb.append(value);
        return sb.reverse().toString();
    }

    public void withQuery(String query) {
        this.query = query;
    }

    /**
     * Extending classes must implement this method with all setup logic. Once the {@link AsyncIndexLookup} is configured, the method should call
     * {@link #executeLookup(AsyncIndexLookup)}.
     */
    protected abstract void executeLookup();

    protected void executeLookup(AsyncIndexLookup lookup) {
        lookup.submit();
        this.result = lookup.lookup();
    }

    protected JexlNode parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query).jjtGetChild(0);
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
        return null;
    }

    protected void assertNoResults() {
        Preconditions.checkNotNull(result, "result cannot be null");
        assertTrue(result.isEmpty());
    }

    protected void assertResultFields(Set<String> expected) {
        Preconditions.checkNotNull(result, "result cannot be null");
        assertEquals(expected, result.keySet());
    }

    protected void assertResultValues(String field, Set<String> values) {
        Preconditions.checkNotNull(result, "result cannot be null");
        assertTrue(result.containsKey(field), "result did not contain field: " + field);
        Set<String> resultValues = new HashSet<>(result.get(field));
        assertEquals(values, resultValues);
    }

    protected void assertExceptionSeen() {
        Preconditions.checkNotNull(result, "result cannot be null");
        assertTrue(result.isExceptionSeen());
    }

    protected void assertTimeoutExceeded() {
        Preconditions.checkNotNull(result, "result cannot be null");
        assertTrue(result.isTimeoutExceeded());
    }
}
