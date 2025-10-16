package datawave.query.rules;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import datawave.microservice.query.Query;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.util.TableName;

public class FieldExistenceRuleTest extends ShardQueryRuleTest {

    private static final Set<String> ALL_FIELDS = Set.of("FOO", "BAR", "BAT");
    private static final String[] AUTHS = {"FOO", "BAR", "BAT"};
    private static final Set<Authorizations> AUTHS_SET = Collections.singleton(new Authorizations(AUTHS));
    private static final String ANYFIELD = "_ANYFIELD_";
    private static final MockMetadataHelper defaultMetadataHelper = new MockMetadataHelper(AUTHS_SET, AUTHS_SET);

    private final Set<String> fieldExceptions = new HashSet<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        defaultMetadataHelper.addFields(ALL_FIELDS);
    }

    @BeforeEach
    public void setUp() throws Exception {
        givenQuerySettings(Mockito.mock(Query.class));
        Mockito.doReturn(new Date(0L)).when(querySettings).getBeginDate();
        Mockito.doReturn(new Date(4070908800L)).when(querySettings).getEndDate();
        givenRuleName(RULE_NAME);
        givenMetadataHelper(defaultMetadataHelper);
        expectRuleName(RULE_NAME);
        if (!defaultMetadataHelper.getAccumuloClient().tableOperations().exists(TableName.METADATA)) {
            defaultMetadataHelper.getAccumuloClient().tableOperations().create(TableName.METADATA);
        }
    }

    /**
     * Test a query where all fields exist.
     */
    @Test
    public void testAllFieldsExist() throws Exception {
        givenQuery("FOO == 'abc' || BAR =~ 'abc' || filter:includeRegex(BAT, '45*')");
        expectMessage("Fields not ingested in provided date range: BAR, BAT, FOO");
        assertResult();
    }

    /**
     * Test a query where some fields do not exist.
     */
    @Test
    public void testNonExistentFields() throws Exception {
        givenQuery("TOMFOOLERY == 'abc' || CHAOS =~ 'abc' || filter:includeRegex(SHENANIGANS, '45.8') || FOO == 'aa'");
        expectMessage("Fields not found in data dictionary: TOMFOOLERY, CHAOS, SHENANIGANS");
        expectMessage("Fields not ingested in provided date range: CHAOS, TOMFOOLERY, FOO, SHENANIGANS");
        assertResult();
    }

    /**
     * Test a query that has a non-existent field that is a special field.
     */
    @Test
    public void testSpecialField() throws Exception {
        givenQuery("FOO == 'abc' || TOMFOOLERY == 'abc' || _ANYFIELD_ = 'abc'");
        givenFieldException(ANYFIELD);
        expectMessage("Fields not found in data dictionary: TOMFOOLERY");
        expectMessage("Fields not ingested in provided date range: TOMFOOLERY, FOO");
        assertResult();
    }

    /**
     * Test a scenario where an exception gets thrown by the metadata helper.
     */
    @Test
    public void testExceptionThrown() throws Exception {
        MetadataHelper mockHelper = EasyMock.createMock(MetadataHelper.class);
        Exception exception = new IllegalArgumentException("Failed to fetch all fields");
        EasyMock.expect(mockHelper.getAllFields(Collections.emptySet())).andThrow(exception);
        EasyMock.replay(mockHelper);

        givenQuery("FOO == 'abc'");
        givenMetadataHelper(mockHelper);

        expectException(exception);
        assertResult();
    }

    private void givenFieldException(String exception) {
        this.fieldExceptions.add(exception);
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToJexl();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        FieldExistenceRule rule = new FieldExistenceRule(ruleName);
        rule.setSpecialFields(fieldExceptions);
        return rule;
    }
}
