package datawave.query.jexl.visitors;

import static datawave.query.Constants.ANY_FIELD;
import static datawave.query.Constants.NO_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MetadataHelper;
import datawave.util.time.DateHelper;

@ExtendWith(EasyMockExtension.class)
public class FieldMissingFromDateRangeVisitorTest {

    private static final String beginDate = "20201231";
    private static final String endDate = "20251231";
    private static final Set<String> specialFields = Set.of(ANY_FIELD, NO_FIELD);

    @Mock
    private MetadataHelper helper;

    private String query;

    private final Set<String> dataTypes = new HashSet<>();
    private final Set<String> expectedImmaterialFields = new LinkedHashSet<>();

    private boolean helperMocked = false;

    @AfterEach
    void tearDown() {
        this.query = null;
        this.dataTypes.clear();
        this.expectedImmaterialFields.clear();
    }

    /**
     * Test query with ANDed fields where all exist during the date range.
     */
    @Test
    public void testWithAndFieldsThatAllExist() throws ParseException {
        givenQuery("AGE == 'foo' && GENDER == 'bar'");

        expectHelperToBeCalled(Set.of("AGE", "GENDER"), Set.of());

        assertResult();
    }

    /**
     * Test query with multiple ORed fields that all exist during the date range.
     */
    @Test
    public void testWithORFieldsThatAllExist() throws ParseException {
        givenQuery("AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "JOB"), Set.of());

        assertResult();
    }

    /**
     * Test query with ANDed fields but only some exist during the date range.
     */
    @Test
    public void testWithAndFieldsThatSomeExist() throws ParseException {
        givenQuery("AGE == 'foo' && GENDER == 'bar' && JOB == 'foo'");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "JOB"), Set.of("JOB"));

        expectImmaterialFields("JOB");

        assertResult();
    }

    /**
     * Test query with ANDed fields but only some exist during the date range with a datatype filter.
     */
    @Test
    public void testWithAndFieldsThatSomeExistWithNonEmptyDataTypes() throws ParseException {
        givenQuery("AGE == 'foo' && GENDER == 'bar' && JOB == 'foo'");
        givenDataTypes("attr");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "JOB"), Set.of("JOB"));

        expectImmaterialFields("JOB");

        assertResult();
    }

    /**
     * Test query with only special fields ANDed.
     */
    @Test
    public void testAndWithOnlySpecialFields() throws ParseException {
        givenQuery("_ANYFIELD_ == 'foo' && _NOFIELD_ == 'bar'");

        assertResult();
    }

    /**
     * Test query with only special fields ORd.
     */
    @Test
    public void testOrWithOnlySpecialFields() throws ParseException {
        givenQuery("_ANYFIELD_ == 'foo' || _NOFIELD_ == 'bar'");

        assertResult();
    }

    /**
     * Test query with ORed fields but only some exist during the date range. Fields will only be returned as NonIngested if ALL ORed fields are not found.
     */
    @Test
    public void testWithOrFieldsThatSomeExist() throws ParseException {
        givenQuery("AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "JOB"), Set.of("AGE", "GENDER"));

        assertResult();
    }

    /**
     * Test query with ORed fields but ALL do not exist during the date range. Fields will only be returned as NonIngested if ALL ORed fields are not found.
     */
    @Test
    public void testWithOrFieldsThatAllDoNotExist() throws ParseException {
        givenQuery("AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "JOB"), Set.of("AGE", "GENDER", "JOB"));

        expectImmaterialFields("AGE", "GENDER", "JOB");

        assertResult();
    }

    /**
     * Test query with function for a field that exists in the date range.
     */
    @Test
    public void testRegexFunctionWithFieldThatExists() throws ParseException {
        givenQuery("filter:includeRegex(GENDER, 'bar.*')");

        expectHelperToBeCalled(Set.of("GENDER"), Set.of());

        assertResult();
    }

    /**
     * Test query with function for a special field.
     */
    @Test
    public void testRegexFunctionWithSpecialField() throws ParseException {
        givenQuery("filter:includeRegex(_ANYFIELD_, 'bar.*')");

        assertResult();
    }

    /**
     * Test query with function for a field that does not exist in the date range.
     */
    @Test
    public void testRegexFunctionWithFieldThatDoesNotExist() throws ParseException {
        givenQuery("filter:includeRegex(JOB, 'bar.*')");

        expectHelperToBeCalled(Set.of("JOB"), Set.of("JOB"));

        expectImmaterialFields("JOB");

        assertResult();
    }

    @Test
    void testNestedFunction() throws ParseException {
        givenQuery("AGE == 'foo' && (NAME == 'bar' || filter:includeRegex(JOB, 'bar.*'))");

        expectHelperToBeCalled(Set.of("AGE", "NAME", "JOB"), Set.of("NAME", "JOB"));

        expectImmaterialFields("JOB", "NAME");

        assertResult();
    }

    @Test
    public void testNestedIntersectionWithSomeMissingFields() throws ParseException {
        givenQuery("AGE == 'foo' || (GENDER == 'bar' && JOB == 'foo')");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "JOB"), Set.of("GENDER", "JOB"));

        assertResult();
    }

    @Test
    public void testNestedUnion() throws ParseException {
        givenQuery("AGE == 'foo' && (GENDER == 'bar' || JOB == 'foo')");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "JOB"), Set.of("GENDER", "JOB"));

        expectImmaterialFields("GENDER", "JOB");

        assertResult();
    }

    @Test
    public void testDoubleNestedIntersectionWhereSomeMissing() throws ParseException {
        givenQuery("(AGE == 'foo' && GENDER == 'foo') || (GENDER == 'bar' && JOB == 'foo')");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "JOB"), Set.of("AGE", "GENDER"));

        assertResult();
    }

    @Test
    public void testDoubleNestedIntersectionWhereAllMissing() throws ParseException {
        givenQuery("(AGE == 'foo' && GENDER == 'foo') || (GENDER == 'bar' && JOB == 'foo')");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "JOB"), Set.of("AGE", "GENDER", "JOB"));

        expectImmaterialFields("AGE", "GENDER", "JOB");

        assertResult();
    }

    @Test
    public void testDoubleNestedUnionWithSomeORsMissingAllFields() throws ParseException {
        givenQuery("(AGE == 'foo' || GENDER == 'foo') && (NAME == 'bar' || JOB == 'foo') && (ORG == 'hr' || NAME == 'hat')");

        expectHelperToBeCalled(Set.of("AGE", "GENDER", "NAME", "JOB", "ORG"), Set.of("AGE", "GENDER", "NAME", "JOB"));

        expectImmaterialFields("AGE", "GENDER", "NAME", "JOB");

        assertResult();
    }

    private void assertResult() throws ParseException {
        // If we expect any fields to be found, mock up a call to the helper function.
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        Set<String> actualImmaterialFields = FieldMissingFromDateRangeVisitor.getNonIngestedFields(this.helper, script, dataTypes, specialFields,
                        DateHelper.parse(beginDate), DateHelper.parse(endDate));

        if (helperMocked) {
            // If the helper function was mocked, verify that the arguments to the function matched what we expected.
            EasyMock.verify(this.helper);
        }

        assertThat(actualImmaterialFields).isEqualTo(expectedImmaterialFields);
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expectHelperToBeCalled(Set<String> foundFields, Set<String> missingFields) {
        EasyMock.expect(this.helper.getMissingFieldsInDateRange(foundFields, dataTypes, beginDate, endDate, specialFields)).andReturn(missingFields);
        EasyMock.replay(this.helper);
        helperMocked = true;
    }

    private void givenDataTypes(String... dataTypes) {
        this.dataTypes.addAll(Arrays.asList(dataTypes));
    }

    private void expectImmaterialFields(String... fields) {
        this.expectedImmaterialFields.addAll(Arrays.asList(fields));
    }
}
