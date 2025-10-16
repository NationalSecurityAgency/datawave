package datawave.query.jexl.visitors;

import static datawave.data.ColumnFamilyConstants.COLF_F;
import static datawave.query.Constants.ANY_FIELD;
import static datawave.query.Constants.NO_FIELD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import datawave.microservice.query.Query;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.DateFrequencyMap;
import datawave.query.util.MockMetadataHelper;
import datawave.util.TableName;

public class FieldMissingFromDateRangeVisitorTest {
    private static final String[] AUTHS = {"FOO", "BAR"};
    private static final Set<Authorizations> AUTHS_SET = Collections.singleton(new Authorizations(AUTHS));
    private static final String NULL_BYTE = "\0";
    private static final String AGGREGATED = "AGG";
    private final MockMetadataHelper helper = new MockMetadataHelper(AUTHS_SET, AUTHS_SET);
    private final List<Mutation> mutations = new ArrayList<>();
    private Query querySettings;

    // Special fields required by visitor.
    private final Set<String> specialFields = Sets.newHashSet(ANY_FIELD, NO_FIELD);

    @BeforeEach
    public void setUp() throws Exception {
        querySettings = Mockito.mock(Query.class);
        if (!helper.getAccumuloClient().tableOperations().exists(TableName.METADATA)) {
            helper.getAccumuloClient().tableOperations().create(TableName.METADATA);
        }

        givenAggregatedRow("AGE", COLF_F, "num", "FOO", 1499999999L, createDateFrequencyMap("20210101", 40L, "20210102", 15L, "20210103", 20L));
        givenAggregatedRow("AGE", COLF_F, "lifetime", "FOO", 1499999999L, createDateFrequencyMap("20220101", 40L, "20220102", 15L, "20220103", 20L));
        givenAggregatedRow("AGE", COLF_F, "var", "BAR", 1499999999L, createDateFrequencyMap("20230101", 40L, "20230102", 15L, "20230103", 20L));
        givenAggregatedRow("GENDER", COLF_F, "text", "FOO", 1499999999L, createDateFrequencyMap("20240101", 40L, "20240102", 15L, "20240103", 20L));
        givenAggregatedRow("JOB", COLF_F, "attr", "FOO", 1499999999L, createDateFrequencyMap("20250101", 40L, "20250102", 15L, "20250103", 20L));

        writeMutations(helper.getAccumuloClient(), this.mutations);

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
     * Test query with only special fields ANDed.
     */
    @Test
    public void testAndWithOnlySpecialFields() throws ParseException {
        String query = "_ANYFIELD_ == 'foo' && _NOFIELD_ == 'bar'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1704153600000L); // 01/02/2024 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet(), Sets.newHashSet());
    }

    /**
     * Test query with only special fields ORd.
     */
    @Test
    public void testOrWithOnlySpecialFields() throws ParseException {
        String query = "_ANYFIELD_ == 'foo' || _NOFIELD_ == 'bar'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);

        Date begin = new Date(1609372800000L); // 12/31/2020 00:00:00 GMT
        Date end = new Date(1704153600000L); // 01/02/2024 00:00:00 GMT

        runCheck(script, begin, end, Sets.newHashSet(), Sets.newHashSet());
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
     * Test query with multiple ANDed fields where some will be missed due to the datatype filter. The special field will be excluded.
     */
    @Test
    public void testOneMissingAndFieldBecauseOfDatatypeFilterWithSpecialField() throws ParseException {
        String query = "AGE == 'foo' && GENDER == 'bar' && JOB == 'foo' && _ANYFIELD_ == 'test'";
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
     * Test query with multiple ORed fields where ALL will be missed due to the datatype filter. The special field will be excluded. Fields will only be
     * returned as NonIngested if ALL ORed fields are not found.
     */
    @Test
    public void testAllMissingORFieldBecauseOfDatatypeFilterWithSpecialField() throws ParseException {
        String query = "AGE == 'foo' || GENDER == 'bar' || JOB == 'foo' || _ANYFIELD_ = 'abc'";
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
     * Test query with function for a special field.
     */
    @Test
    public void testRegexFunctionWithSpecialField() throws ParseException {
        String query = "filter:includeRegex(_ANYFIELD_, 'bar.*')";
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

    public static DateFrequencyMap createDateFrequencyMap(Object... entries) {
        DateFrequencyMap map = new DateFrequencyMap();
        int lastEntry = entries.length - 1;
        for (int i = 0; i < lastEntry; i++) {
            String date = (String) entries[i];
            i++;
            long count = (Long) entries[i];
            map.put(date, count);
        }
        return map;
    }

    private void givenAggregatedRow(String row, Text colf, String datatype, String colv, long timestamp, DateFrequencyMap map) {
        givenMutation(row, colf, datatype + NULL_BYTE + AGGREGATED, colv, timestamp, new Value(WritableUtils.toByteArray(map)));
    }

    private void givenMutation(String row, Text colf, String colq, String colv, long timestamp, Value value) {
        Mutation mutation = new Mutation(row);
        mutation.put(colf, new Text(colq), new ColumnVisibility(colv), timestamp, value);
        this.mutations.add(mutation);
    }

    private void writeMutations(AccumuloClient client, Collection<Mutation> mutations) {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = client.createBatchWriter(TableName.METADATA, config)) {
            writer.addMutations(mutations);
            writer.flush();
        } catch (MutationsRejectedException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
