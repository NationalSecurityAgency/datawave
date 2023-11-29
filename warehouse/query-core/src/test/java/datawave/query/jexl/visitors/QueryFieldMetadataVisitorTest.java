package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.QueryFieldMetadataVisitor.FieldMetadata;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryFieldMetadataVisitorTest {

    private final Set<String> indexedFields = Sets.newHashSet("FOO");
    private final Set<String> indexOnlyFields = Sets.newHashSet("FOO");
    private final Set<String> tokenizedFields = Sets.newHashSet("FOO");
    private QueryFieldMetadataVisitor visitor;

    @Test
    public void testSimpleQuery() {
        String query = "FOO == 'bar'";
        applyVisitor(query);

        FieldMetadata fieldMetadata = visitor.getFieldMetadata();
        assertFalse(fieldMetadata.isFieldContentFunction("FOO"));
        assertFalse(fieldMetadata.isFieldFilterFunction("FOO"));
        assertFalse(fieldMetadata.isFieldQueryFunction("FOO"));
        assertFalse(fieldMetadata.isFieldGeoFunction("FOO"));
        assertFalse(fieldMetadata.isFieldRegexTerm("FOO"));
    }

    @Test
    public void testFieldIsRegexEquals() {
        String query = "FOO =~ 'ba.*'";
        applyVisitor(query);
        assertFieldIsRegex("FOO");
    }

    @Test
    public void testFieldIsRegexNotEquals() {
        String query = "FOO !~ 'ba.*'";
        applyVisitor(query);
        assertFieldIsRegex("FOO");
    }

    @Test
    public void testContentFunctions() {
        //  @formatter:off
        String[] queries = {
                        "content:phrase(FOO, termOffsetMap, 'fizz', 'buzz')",
                        "content:within(FOO, 2, termOffsetMap, 'fizz', 'buzz')",
                        "content:adjacent(FOO, termOffsetMap, 'fizz', 'buzz')" };
        //  @formatter:on

        for (String query : queries) {
            applyVisitor(query);
            assertFieldIsContentFunction("FOO");
        }
    }

    @Test
    public void testFilterFunctions() {
        String[] queries = {"filter:includeRegex(FOO, 'ba.*')", "filter:excludeRegex(FOO, 'ba.*')"};

        for (String query : queries) {
            applyVisitor(query);
            assertFieldIsFilterFunction("FOO");
        }
    }

    @Test
    public void testFieldIsGeoWaveFunction() {
        String[] queries = {"geowave:intersects(FOO, 'POINT(4 4)')", "geowave:overlaps(FOO, 'POINT(5 5)')"};

        for (String query : queries) {
            applyVisitor(query);
            assertFieldIsGeoFunction("FOO");
        }
    }

    @Test
    public void testQueryFunctions() {
        String[] queries = {"f:includeText(FOO, 'bar')", "f:matchRegex(FOO, 'ba.*')"};

        for (String query : queries) {
            applyVisitor(query);
            assertFieldIsQueryFunction("FOO");
        }
    }

    @Test
    public void testGroupingFunctions() {
        //  @formatter:off
        String[] queries = {
                        "grouping:matchesInGroup(FOO, 'bar')",
                        "grouping:matchesInGroupLeft(FOO, 'bar')",
                        "grouping:atomValuesMatch(FOO, FOO2)"
        };
        //  @formatter:on

        for (String query : queries) {
            applyVisitor(query);
            assertFieldIsGroupingFunction("FOO");
        }
    }

    private void applyVisitor(String query) {
        ASTJexlScript script = parseQuery(query);
        visitor = new QueryFieldMetadataVisitor(indexedFields, indexOnlyFields, tokenizedFields);
        script.jjtAccept(visitor, null);
    }

    private ASTJexlScript parseQuery(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private void assertFieldIsContentFunction(String field) {
        FieldMetadata fieldMetadata = visitor.getFieldMetadata();
        assertTrue(fieldMetadata.isFieldContentFunction(field));
        assertFalse(fieldMetadata.isFieldFilterFunction(field));
        assertFalse(fieldMetadata.isFieldQueryFunction(field));
        assertFalse(fieldMetadata.isFieldGeoFunction(field));
        assertFalse(fieldMetadata.isFieldGroupingFunction(field));
        assertFalse(fieldMetadata.isFieldRegexTerm(field));
    }

    private void assertFieldIsFilterFunction(String field) {
        FieldMetadata fieldMetadata = visitor.getFieldMetadata();
        assertFalse(fieldMetadata.isFieldContentFunction(field));
        assertTrue(fieldMetadata.isFieldFilterFunction(field));
        assertFalse(fieldMetadata.isFieldQueryFunction(field));
        assertFalse(fieldMetadata.isFieldGeoFunction(field));
        assertFalse(fieldMetadata.isFieldGroupingFunction(field));
        assertFalse(fieldMetadata.isFieldRegexTerm(field));
    }

    private void assertFieldIsQueryFunction(String field) {
        FieldMetadata fieldMetadata = visitor.getFieldMetadata();
        assertFalse(fieldMetadata.isFieldContentFunction(field));
        assertFalse(fieldMetadata.isFieldFilterFunction(field));
        assertTrue(fieldMetadata.isFieldQueryFunction(field));
        assertFalse(fieldMetadata.isFieldGeoFunction(field));
        assertFalse(fieldMetadata.isFieldGroupingFunction(field));
        assertFalse(fieldMetadata.isFieldRegexTerm(field));
    }

    private void assertFieldIsGeoFunction(String field) {
        FieldMetadata fieldMetadata = visitor.getFieldMetadata();
        assertFalse(fieldMetadata.isFieldContentFunction(field));
        assertFalse(fieldMetadata.isFieldFilterFunction(field));
        assertFalse(fieldMetadata.isFieldQueryFunction(field));
        assertTrue(fieldMetadata.isFieldGeoFunction(field));
        assertFalse(fieldMetadata.isFieldGroupingFunction(field));
        assertFalse(fieldMetadata.isFieldRegexTerm(field));
    }

    private void assertFieldIsGroupingFunction(String field) {
        FieldMetadata fieldMetadata = visitor.getFieldMetadata();
        assertFalse(fieldMetadata.isFieldContentFunction(field));
        assertFalse(fieldMetadata.isFieldFilterFunction(field));
        assertFalse(fieldMetadata.isFieldQueryFunction(field));
        assertFalse(fieldMetadata.isFieldGeoFunction(field));
        assertTrue(fieldMetadata.isFieldGroupingFunction(field));
        assertFalse(fieldMetadata.isFieldRegexTerm(field));
    }

    private void assertFieldIsRegex(String field) {
        FieldMetadata fieldMetadata = visitor.getFieldMetadata();
        assertFalse(fieldMetadata.isFieldContentFunction(field));
        assertFalse(fieldMetadata.isFieldFilterFunction(field));
        assertFalse(fieldMetadata.isFieldQueryFunction(field));
        assertFalse(fieldMetadata.isFieldGeoFunction(field));
        assertFalse(fieldMetadata.isFieldGroupingFunction(field));
        assertTrue(fieldMetadata.isFieldRegexTerm(field));
    }

}
