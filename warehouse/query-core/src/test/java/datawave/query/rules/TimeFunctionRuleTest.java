package datawave.query.rules;

import java.util.Set;

import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.type.DateType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.query.util.MockMetadataHelper;
import datawave.query.util.TypeMetadata;

class TimeFunctionRuleTest extends ShardQueryRuleTest {

    private static final Set<String> DATE_TYPE = Set.of(DateType.class.getName());
    private static final Set<String> LC_NO_DIACRITICS_TYPE = Set.of(LcNoDiacriticsType.class.getName());

    @BeforeEach
    void setUp() {
        givenRuleName(RULE_NAME);
        expectRuleName(RULE_NAME);
    }

    /**
     * Test a query that has no time functions.
     */
    @Test
    void testQueryWithoutTimeFunctions() throws Exception {
        givenQuery("FOO == 'abc'");

        // Do not expect any messages.
        assertResult();
    }

    /**
     * Test a query with a time function that reference date type fields.
     */
    @Test
    void testTimeFunctionWithDateTypeField() throws Exception {
        givenQuery("filter:timeFunction(DATE1,DATE2,'-','>',2522880000000L)");
        givenMetadataHelper(new MockMetadataHelper());

        // Set up a mock TypeMetadata that will return the date type for the fields in the time function.
        TypeMetadata typeMetadata = EasyMock.mock(TypeMetadata.class);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("DATE1")).andReturn(DATE_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("DATE2")).andReturn(DATE_TYPE);
        EasyMock.replay(typeMetadata);
        givenTypeMetadata(typeMetadata);

        // Do not expect any messages.
        assertResult();
    }

    /**
     * Test a query with a time function that references one date type field, and one non-date type field.
     */
    @Test
    void testTimeFunctionWithOneNonDateTypeField() throws Exception {
        givenQuery("filter:timeFunction(DATE1,NON_DATE2,'-','>',2522880000000L)");
        givenMetadataHelper(new MockMetadataHelper());

        // Set up a mock TypeMetadata that will return non-date types for the fields in the time function.
        TypeMetadata typeMetadata = EasyMock.mock(TypeMetadata.class);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("DATE1")).andReturn(DATE_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("NON_DATE2")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.replay(typeMetadata);
        givenTypeMetadata(typeMetadata);

        expectMessage("Function #TIME_FUNCTION (filter:timeFunction) found with fields that are not date types: NON_DATE2");

        assertResult();
    }

    /**
     * Test a query with a time function that references only non-date type fields.
     */
    @Test
    void testTimeFunctionWithBothNonDateTypeField() throws Exception {
        givenQuery("filter:timeFunction(NON_DATE1,NON_DATE2,'-','>',2522880000000L)");
        givenMetadataHelper(new MockMetadataHelper());

        // Set up a mock TypeMetadata that will return non-date types for the fields in the time function.
        TypeMetadata typeMetadata = EasyMock.mock(TypeMetadata.class);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("NON_DATE1")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("NON_DATE2")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.replay(typeMetadata);
        givenTypeMetadata(typeMetadata);

        expectMessage("Function #TIME_FUNCTION (filter:timeFunction) found with fields that are not date types: NON_DATE1, NON_DATE2");

        assertResult();
    }

    /**
     * Test a query with multiple time functions.
     */
    @Test
    void testMultipleTimeFunctions() throws Exception {
        givenQuery("filter:timeFunction(DATE1,DATE2,'-','>',2522880000000L) && filter:timeFunction(NON_DATE1,DATE2,'-','>',2522880000000L) && filter:timeFunction(NON_DATE2,NON_DATE3,'-','>',2522880000000L)");
        givenMetadataHelper(new MockMetadataHelper());

        // Set up a mock TypeMetadata that will return non-date types for the fields in the time function.
        TypeMetadata typeMetadata = EasyMock.mock(TypeMetadata.class);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("DATE1")).andReturn(DATE_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("DATE2")).andReturn(DATE_TYPE).times(2);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("NON_DATE1")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("NON_DATE2")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.expect(typeMetadata.getNormalizerNamesForField("NON_DATE3")).andReturn(LC_NO_DIACRITICS_TYPE);
        EasyMock.replay(typeMetadata);
        givenTypeMetadata(typeMetadata);

        expectMessage("Function #TIME_FUNCTION (filter:timeFunction) found with fields that are not date types: NON_DATE1, NON_DATE2, NON_DATE3");

        assertResult();
    }

    @Override
    protected Object parseQuery() throws Exception {
        return parseQueryToJexl();
    }

    @Override
    protected ShardQueryRule getNewRule() {
        return new TimeFunctionRule(ruleName);
    }
}
