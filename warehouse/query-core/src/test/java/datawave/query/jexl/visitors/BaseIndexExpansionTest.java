package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MockMetadataHelper;
import datawave.util.TableName;
import datawave.util.time.DateHelper;

/**
 * Collection of common methods used for testing the following visitors
 * <ul>
 * <li>{@link RegexIndexExpansionVisitor}</li>
 * <li>{@link UnfieldedIndexExpansionVisitor}</li>
 * <li>{@link BoundedRangeIndexExpansionVisitor}</li>
 * </ul>
 */
public abstract class BaseIndexExpansionTest {

    private static final Logger log = LoggerFactory.getLogger(BaseIndexExpansionTest.class);

    protected static final String DEFAULT_DATE = "20250606";
    protected static final String DEFAULT_DATATYPE = "datatype";
    protected static final Value EMPTY_VALUE = new Value();

    private static final Authorizations AUTHS = new Authorizations("ALL");

    protected static AccumuloClient client;
    protected static TableOperations tops;

    private final StringBuilder sb = new StringBuilder();

    // required for each visitor call
    protected ShardQueryConfiguration config;
    protected ScannerFactory scannerFactory;
    protected MockMetadataHelper helper;
    protected Map<String,IndexLookup> lookupMap;

    @BeforeAll
    public static void setup() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        client = new InMemoryAccumuloClient("root", instance);
        tops = client.tableOperations();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        createOrRecreateTable(TableName.SHARD_INDEX);
        createOrRecreateTable(TableName.SHARD_RINDEX);

        // create fresh config with default dates
        config = new ShardQueryConfiguration();
        config.setClient(client);
        config.setExpandFields(true);
        config.setExpandValues(true);
        config.setBeginDate(DateHelper.parse("20250606"));
        config.setEndDate(DateHelper.parse("20250606"));
        config.setAuthorizations(Set.of(AUTHS));

        scannerFactory = new ScannerFactory(client);
        helper = new MockMetadataHelper();
        lookupMap = new HashMap<>();
    }

    /**
     * Creates a table, or drop the table and recreates it
     *
     * @param tableName
     *            the table
     * @throws Exception
     *             if something goes wrong
     */
    private void createOrRecreateTable(String tableName) throws Exception {
        if (tops.exists(tableName)) {
            tops.delete(tableName);
        }
        tops.create(tableName);
    }

    /**
     * A wrapper around {@link #expand(ASTJexlScript)} that handles running the expansion and verifying the result
     *
     * @param query
     *            the input query
     * @param expected
     *            the expected result
     */
    protected void driveExpansion(String query, String expected) throws Exception {
        ASTJexlScript script = parse(query);
        JexlNode node = expand(script);

        JexlNode expectedTree = parse(expected);
        boolean equivalent = TreeEqualityVisitor.isEqual(expectedTree, node);
        if (!equivalent) {
            String result = JexlStringBuildingVisitor.buildQuery(node);
            log.info("expected: {}", expected);
            log.info("result  : {}", result);
            fail("result query did not match expectation");
        }
    }

    /**
     * All extending classes must override this method and determine how a query tree is expanded
     *
     * @param script
     *            the Jexl script
     * @return a (potentially) expanded script
     */
    protected abstract JexlNode expand(ASTJexlScript script) throws Exception;

    /**
     * Parse a query string into a Jexl script
     *
     * @param query
     *            the query
     * @return a Jexl script
     */
    protected ASTJexlScript parse(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            fail("failed to parse query: " + query);
        }
        return null;
    }

    /**
     * Write a field and value to the shard index with default date, datatype and value
     *
     * @param value
     *            the field value
     * @param field
     *            the field name
     */
    public void write(String value, String field) {
        write(value, field, DEFAULT_DATE, DEFAULT_DATATYPE, EMPTY_VALUE);
    }

    /**
     * Write a field name, field value, and {@link Value} to the shard index with a default date and datatype
     *
     * @param value
     *            the field value
     * @param field
     *            the field name
     * @param v
     *            the value
     */
    public void write(String value, String field, Value v) {
        write(value, field, DEFAULT_DATE, DEFAULT_DATATYPE, v);
    }

    /**
     * Write a field, value, date and datatype to the shard index
     *
     * @param value
     *            the field value
     * @param field
     *            the field name
     * @param date
     *            the date
     * @param datatype
     *            the datatype
     */
    public void write(String value, String field, String date, String datatype) {
        write(value, field, date, datatype, EMPTY_VALUE);
    }

    /**
     * Write a field value, field name, date, datatype and value to the shard index
     *
     * @param value
     *            the field value
     * @param field
     *            the field name
     * @param date
     *            the date
     * @param datatype
     *            the datatype
     * @param v
     *            the value
     */
    public void write(String value, String field, String date, String datatype, Value v) {
        try (var writer = client.createBatchWriter(TableName.SHARD_INDEX)) {
            Mutation m = new Mutation(value);
            m.put(field, date + "\0" + datatype, v);
            writer.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Write a field and value to the shard reverse index with default date, datatype and {@link Value}
     *
     * @param value
     *            the field value
     * @param field
     *            the field name
     */
    public void writeReverse(String value, String field) {
        writeReverse(value, field, DEFAULT_DATE, DEFAULT_DATATYPE, EMPTY_VALUE);
    }

    /**
     * Write a field name, field value, and {@link Value} to the shard reverse index
     *
     * @param value
     *            the field value
     * @param field
     *            the field name
     * @param v
     *            the value
     */
    public void writeReverse(String value, String field, Value v) {
        writeReverse(value, field, DEFAULT_DATE, DEFAULT_DATATYPE, v);
    }

    /**
     * Write a field name, value, date and datatype to the shard reverse index
     *
     * @param value
     *            the field value
     * @param field
     *            the field name
     * @param date
     *            the date
     * @param datatype
     *            the datatype
     */
    public void writeReverse(String value, String field, String date, String datatype) {
        writeReverse(value, field, date, datatype, EMPTY_VALUE);
    }

    /**
     * Write a field value, field name, date, datatype and {@link Value} to the shard reverse index
     *
     * @param value
     *            the field value
     * @param field
     *            the field name
     * @param date
     *            the date
     * @param datatype
     *            the datatype
     * @param v
     *            the value
     */
    public void writeReverse(String value, String field, String date, String datatype, Value v) {
        try (var writer = client.createBatchWriter(TableName.SHARD_RINDEX)) {
            Mutation m = new Mutation(reverse(value));
            m.put(field, date + "\0" + datatype, v);
            writer.addMutation(m);
        } catch (TableNotFoundException | MutationsRejectedException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Utility method to reverse a value, used when writing a key to the shard reverse index
     *
     * @param value
     *            the value
     * @return the reversed value
     */
    private String reverse(String value) {
        sb.setLength(0);
        sb.append(value);
        return sb.reverse().toString();
    }
}
