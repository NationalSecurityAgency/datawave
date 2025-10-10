package datawave.query.jexl.visitors;

import static datawave.data.ColumnFamilyConstants.COLF_F;
import static datawave.query.Constants.ANY_FIELD;
import static datawave.query.Constants.NO_FIELD;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.microservice.query.Query;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.AllFieldMetadataHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.TypeMetadataHelper;

public class FieldMissingFromDateRangeVisitorTest {
    private MetadataHelper helper;
    private Query querySettings;
    private AccumuloClient accumuloClient;

    // Special fields required by visitor.
    private final Set<String> specialFields = Sets.newHashSet(ANY_FIELD, NO_FIELD);
    private final List<Mutation> mutations = new ArrayList<>();

    private static final String TABLE_METADATA = "metadata";
    private static final String[] AUTHS = {"FOO", "BAR"};
    private static final Set<Authorizations> AUTHS_SET = Collections.singleton(new Authorizations(AUTHS));
    private static final String NULL_BYTE = "\0";

    @BeforeEach
    public void setUp() throws Exception {
        File dir = new File(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(".")).toURI());
        File targetDir = dir.getParentFile();
        System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());

        querySettings = Mockito.mock(Query.class);
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(FieldMissingFromDateRangeVisitorTest.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_METADATA)) {
            accumuloClient.tableOperations().create(TABLE_METADATA);
        }
        helper = new MetadataHelper(createAllFieldMetadataHelper(accumuloClient), AUTHS_SET, accumuloClient, TABLE_METADATA, AUTHS_SET, AUTHS_SET);

        givenNonAggregatedRow("AGE", COLF_F, "num", "FOO", 1500000004L, "20210101", 1L);
        givenNonAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1500000004L, "20220101", 1L);
        givenNonAggregatedRow("AGE", COLF_F, "var", "BAR", 1500000004L, "20230101", 1L);
        givenNonAggregatedRow("GENDER", COLF_F, "text", "FOO", 1500000004L, "20240101", 1L);
        givenNonAggregatedRow("JOB", COLF_F, "attr", "FOO", 1500000004L, "20250101", 1L);

        writeMutations(accumuloClient, this.mutations);
    }

    @AfterEach
    public void tearDown() throws Exception {
        accumuloClient.tableOperations().deleteRows(TABLE_METADATA, null, null);
    }

    /**
     * Test query with ANDed fields where all exist during the date range.
     */
    @Test
    public void testWithAndFieldsThatAllExist() throws ParseException {
        String query = "AGE == 'foo' && GENDER == 'bar'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1735776000000L); // 01/02/2025 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("num", "text"), Sets.newHashSet());
    }

    /**
     * Test query with multiple ORed fields that all exist during the date range.
     */
    @Test
    public void testWithORFieldsThatAllExist() throws ParseException {
        String query = "AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1735776000000L); // 01/02/2025 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("num", "text", "attr"), Sets.newHashSet());
    }

    /**
     * Test query with ANDed fields where all exist during the date range and no DataType Filter is given.
     */
    @Test
    public void testWithAndFieldsThatAllExistWithoutDataTypeFilter() throws ParseException {
        String query = "AGE == 'foo' && GENDER == 'bar'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1735776000000L); // 01/02/2025 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet(), Sets.newHashSet());
    }

    /**
     * Test query with ORed fields where all exist during the date range and no DataType Filter is given.
     */
    @Test
    public void testWithORFieldsThatAllExistWithoutDataTypeFilter() throws ParseException {
        String query = "AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1735776000000L); // 01/02/2025 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet(), Sets.newHashSet());
    }

    /**
     * Test query with ANDed fields but only some exist during the date range.
     */
    @Test
    public void testWithAndFieldsThatSomeExist() throws ParseException {
        String query = "AGE == 'foo' && GENDER == 'bar' && JOB == 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1704153600000L); // 01/02/2024 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("num", "text", "attr"), Sets.newHashSet("JOB"));
    }

    /**
     * Test query with ORed fields but only some exist during the date range. Fields will only be returned as NonIngested if ALL ORed fields are not found.
     */
    @Test
    public void testWithOrFieldsThatSomeExist() throws ParseException {
        String query = "AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609459200000L); // 01/01/2021 00:00:00 GMT
        Date end = new Date(1704153600000L); // 01/02/2024 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("num", "text", "attr"), Sets.newHashSet());
    }

    /**
     * Test query with ORed fields but ALL do not exist during the date range. Fields will only be returned as NonIngested if ALL ORed fields are not found.
     */
    @Test
    public void testWithOrFieldsThatAllDoNotExist() throws ParseException {
        String query = "AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1546214400000L); // 12/31/2018 00:00:00 GMT
        Date end = new Date(1577923200000L); // 01/02/2020 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("num", "text", "attr"), Sets.newHashSet("AGE", "GENDER", "JOB"));
    }

    /**
     * Test query with multiple ANDed fields where some will be missed due to the datatype filter.
     */
    @Test
    public void testOneMissingAndFieldBecauseOfDatatypeFilter() throws ParseException {
        String query = "AGE == 'foo' && GENDER == 'bar' && JOB == 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1735776000000L); // 01/02/2025 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("num", "attr"), Sets.newHashSet("GENDER"));
    }

    /**
     * Test query with multiple ORed fields where some will be missed due to the datatype filter. Fields will only be returned as NonIngested if ALL ORed fields
     * are not found.
     */
    @Test
    public void testOneMissingORFieldBecauseOfDatatypeFilter() throws ParseException {
        String query = "AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1735776000000L); // 01/02/2025 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("num"), Sets.newHashSet());
    }

    /**
     * Test query with multiple ORed fields where ALL will be missed due to the datatype filter. Fields will only be returned as NonIngested if ALL ORed fields
     * are not found.
     */
    @Test
    public void testAllMissingORFieldBecauseOfDatatypeFilter() throws ParseException {
        String query = "AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1735776000000L); // 01/02/2025 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("foo"), Sets.newHashSet("AGE", "GENDER", "JOB"));
    }

    /**
     * Test query with function for a field that exists in the date range.
     */
    @Test
    public void testRegexFunctionWithFieldThatExists() throws ParseException {
        String query = "filter:includeRegex(GENDER, 'bar.*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1735776000000L); // 01/02/2025 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("text"), Sets.newHashSet());
    }

    /**
     * Test query with function for a field that does not exist in the date range.
     */
    @Test
    public void testRegexFunctionWithFieldThatDoesNotExist() throws ParseException {
        String query = "filter:includeRegex(JOB, 'bar.*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1704153600000L); // 01/02/2024 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("attr"), Sets.newHashSet("JOB"));
    }

    /**
     * Test query with function for a field that does not exist in the date range under the provided datatype filter.
     */
    @Test
    public void testRegexFunctionWithUnknownFieldBecauseOfDatatypeFilter() throws ParseException {
        String query = "filter:includeRegex(GENDER, 'bar.*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1735776000000L); // 01/02/2025 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet("foo"), Sets.newHashSet("GENDER"));
    }

    private void runCheck(ASTJexlScript script, Date begin, Date end, Set<String> filter, Set<String> expected) {
        Mockito.doReturn(begin).when(querySettings).getBeginDate();
        Mockito.doReturn(end).when(querySettings).getEndDate();
        Set<String> actual = FieldMissingFromDateRangeVisitor.getNonIngestedFields(helper, script, filter, specialFields, querySettings);
        Assertions.assertEquals(expected, actual);
    }

    private void givenNonAggregatedRow(String row, Text colf, String datatype, String colv, long timestamp, String date, long count) {
        givenMutation(row, colf, datatype + NULL_BYTE + date, colv, timestamp, new Value(LongCombiner.VAR_LEN_ENCODER.encode(count)));
    }

    private void givenMutation(String row, Text colf, String colq, String colv, long timestamp, Value value) {
        Mutation mutation = new Mutation(row);
        mutation.put(colf, new Text(colq), new ColumnVisibility(colv), timestamp, value);
        this.mutations.add(mutation);
    }

    private void writeMutations(AccumuloClient client, Collection<Mutation> mutations) {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = client.createBatchWriter(TABLE_METADATA, config)) {
            writer.addMutations(mutations);
            writer.flush();
        } catch (MutationsRejectedException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static AllFieldMetadataHelper createAllFieldMetadataHelper(AccumuloClient client) {
        final Set<Authorizations> allMetadataAuths = AUTHS_SET;
        final Set<Authorizations> auths = AUTHS_SET;
        TypeMetadataHelper tmh = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, client, TABLE_METADATA, auths, false);
        CompositeMetadataHelper cmh = new CompositeMetadataHelper(client, TABLE_METADATA, auths);
        return new AllFieldMetadataHelper(tmh, cmh, client, TABLE_METADATA, auths, allMetadataAuths);
    }
}
