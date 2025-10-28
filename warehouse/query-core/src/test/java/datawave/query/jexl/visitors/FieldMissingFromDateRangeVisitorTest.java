package datawave.query.jexl.visitors;

import static datawave.query.Constants.ANY_FIELD;
import static datawave.query.Constants.NO_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
    private final Set<String> foundFields = new HashSet<>();
    private final Set<String> missingFields = new HashSet<>();
    private final List<FieldMissingFromDateRangeVisitor.ImmaterialNode> expectedNodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        this.query = null;
        this.dataTypes.clear();
        this.foundFields.clear();
        this.missingFields.clear();
    }

    /**
     * Test query with ANDed fields where all exist during the date range.
     */
    @Test
    public void testWithAndFieldsThatAllExist() throws ParseException {
        givenQuery("AGE == 'foo' && GENDER == 'bar'");
        expectFieldsToBeFound("AGE", "GENDER");

        assertResult();
    }

    /**
     * Test query with multiple ORed fields that all exist during the date range.
     */
    @Test
    public void testWithORFieldsThatAllExist() throws ParseException {
        givenQuery("AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'");
        expectFieldsToBeFound("AGE", "GENDER", "JOB");

        assertResult();
    }

    /**
     * Test query with ANDed fields but only some exist during the date range.
     */
    @Test
    public void testWithAndFieldsThatSomeExist() throws ParseException {
        givenQuery("AGE == 'foo' && GENDER == 'bar' && JOB == 'foo'");
        expectFieldsToBeFound("AGE", "GENDER", "JOB");
        givenMissingFields("JOB");

        expectIrrelevantNode("JOB == 'foo'", Set.of("JOB"));

        assertResult();
    }

    /**
     * Test query with ANDed fields but only some exist during the date range with a datatype filter.
     */
    @Test
    public void testWithAndFieldsThatSomeExistWithNonEmptyDataTypes() throws ParseException {
        givenQuery("AGE == 'foo' && GENDER == 'bar' && JOB == 'foo'");
        givenDataTypes("attr");

        expectFieldsToBeFound("AGE", "GENDER", "JOB");
        givenMissingFields("JOB");

        expectIrrelevantNode("JOB == 'foo'", Set.of("JOB"));

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
        expectFieldsToBeFound("AGE", "GENDER", "JOB");
        givenMissingFields("AGE", "GENDER");

        assertResult();
    }

    /**
     * Test query with ORed fields but ALL do not exist during the date range. Fields will only be returned as NonIngested if ALL ORed fields are not found.
     */
    @Test
    public void testWithOrFieldsThatAllDoNotExist() throws ParseException {
        givenQuery("AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'");
        expectFieldsToBeFound("AGE", "GENDER", "JOB");
        givenMissingFields("AGE", "GENDER", "JOB");

        expectIrrelevantNode("AGE == 'foo' || GENDER == 'bar' || JOB == 'foo'", Set.of("AGE", "GENDER", "JOB"));

        assertResult();
    }

    /**
     * Test query with function for a field that exists in the date range.
     */
    @Test
    public void testRegexFunctionWithFieldThatExists() throws ParseException {
        givenQuery("filter:includeRegex(GENDER, 'bar.*')");
        expectFieldsToBeFound("GENDER");

        assertResult();
    }

    /**
     * Test query with function for a special field.
     */
    @Test
    public void testRegexFunctionWithSpecialField() throws ParseException {
        givenQuery("filter:includeRegex(_ANYFIELD_, 'bar.*')");
        expectFieldsToBeFound("_ANYFIELD_");

        assertResult();
    }

    /**
     * Test query with function for a field that does not exist in the date range.
     */
    @Test
    public void testRegexFunctionWithFieldThatDoesNotExist() throws ParseException {
        givenQuery("filter:includeRegex(JOB, 'bar.*')");
        expectFieldsToBeFound("JOB");
        givenMissingFields("JOB");

        expectIrrelevantNode("filter:includeRegex(JOB, 'bar.*')", Set.of("JOB"));

        assertResult();
    }

    @Test
    void testNestedFunction() throws ParseException {
        givenQuery("AGE == 'foo' && (NAME == 'bar' || filter:includeRegex(JOB, 'bar.*'))");
        expectFieldsToBeFound("AGE", "NAME", "JOB");
        givenMissingFields("NAME", "JOB");

        expectIrrelevantNode("NAME == 'bar' || filter:includeRegex(JOB, 'bar.*')", Set.of("JOB", "NAME"));

        assertResult();
    }

    @Test
    public void testNestedIntersectionWithSomeMissingFields() throws ParseException {
        givenQuery("AGE == 'foo' || (GENDER == 'bar' && JOB == 'foo')");
        expectFieldsToBeFound("AGE", "GENDER", "JOB");
        givenMissingFields("GENDER", "JOB");

        assertResult();
    }

    @Test
    public void testNestedUnion() throws ParseException {
        givenQuery("AGE == 'foo' && (GENDER == 'bar' || JOB == 'foo')");
        expectFieldsToBeFound("AGE", "GENDER", "JOB");
        givenMissingFields("GENDER", "JOB");

        expectIrrelevantNode("GENDER == 'bar' || JOB == 'foo'", Set.of("GENDER", "JOB"));

        assertResult();
    }

    @Test
    public void testDoubleNestedIntersectionWhereSomeMissing() throws ParseException {
        givenQuery("(AGE == 'foo' && GENDER == 'foo') || (GENDER == 'bar' && JOB == 'foo')");

        expectFieldsToBeFound("AGE", "GENDER", "JOB");
        givenMissingFields("AGE", "GENDER");

        assertResult();
    }

    @Test
    public void testDoubleNestedIntersectionWhereAllMissing() throws ParseException {
        givenQuery("(AGE == 'foo' && GENDER == 'foo') || (GENDER == 'bar' && JOB == 'foo')");

        expectFieldsToBeFound("AGE", "GENDER", "JOB");
        givenMissingFields("AGE", "GENDER", "JOB");

        expectIrrelevantNode("(AGE == 'foo' && GENDER == 'foo') || (GENDER == 'bar' && JOB == 'foo')", Set.of("AGE", "GENDER", "JOB"));

        assertResult();
    }

    @Test
    public void testDoubleNestedUnionWithSomeORsMissingAllFields() throws ParseException {
        givenQuery("(AGE == 'foo' || GENDER == 'foo') && (NAME == 'bar' || JOB == 'foo') && (ORG == 'hr' || NAME == 'hat')");

        expectFieldsToBeFound("AGE", "GENDER", "NAME", "JOB", "ORG");
        givenMissingFields("AGE", "GENDER", "NAME", "JOB");

        expectIrrelevantNode("AGE == 'foo' || GENDER == 'foo'", Set.of("AGE", "GENDER"));
        expectIrrelevantNode("NAME == 'bar' || JOB == 'foo'", Set.of("NAME", "JOB"));

        assertResult();
    }

    private void assertResult() throws ParseException {
        boolean helperMocked = false;

        // If we expect any fields to be found, mock up a call to the helper function.
        if (!foundFields.isEmpty()) {
            EasyMock.expect(this.helper.getMissingFieldsInDateRange(foundFields, dataTypes, beginDate, endDate, specialFields)).andReturn(missingFields);
            EasyMock.replay(this.helper);
            helperMocked = true;
        }

        // Fetch the nodes considered to be irrelevant.
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        List<FieldMissingFromDateRangeVisitor.ImmaterialNode> immaterialNodes = FieldMissingFromDateRangeVisitor.getNonIngestedFields(this.helper, script,
                        dataTypes, specialFields, DateHelper.parse(beginDate), DateHelper.parse(endDate));

        if (helperMocked) {
            // If the helper function was mocked, verify that the arguments to the function matched what we expected.
            EasyMock.verify(this.helper);
        }

        assertThat(immaterialNodes).hasSameSizeAs(expectedNodes);

        for (int i = 0; i < immaterialNodes.size(); i++) {
            FieldMissingFromDateRangeVisitor.ImmaterialNode actual = immaterialNodes.get(i);
            FieldMissingFromDateRangeVisitor.ImmaterialNode expected = expectedNodes.get(i);
            assertThat(JexlStringBuildingVisitor.buildQuery(actual.getNode())).isEqualTo(JexlStringBuildingVisitor.buildQuery(expected.getNode()));
            assertThat(actual.getFields()).containsExactlyInAnyOrderElementsOf(expected.getFields());
        }
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void expectFieldsToBeFound(String... fields) {
        this.foundFields.addAll(Arrays.asList(fields));
    }

    private void givenDataTypes(String... dataTypes) {
        this.dataTypes.addAll(Arrays.asList(dataTypes));
    }

    private void givenMissingFields(String... fields) {
        this.missingFields.addAll(Arrays.asList(fields));
    }

    private void expectIrrelevantNode(String node, Set<String> fields) throws ParseException {
        this.expectedNodes.add(new FieldMissingFromDateRangeVisitor.ImmaterialNode(JexlASTHelper.parseJexlQuery(node), fields));
    }
}
