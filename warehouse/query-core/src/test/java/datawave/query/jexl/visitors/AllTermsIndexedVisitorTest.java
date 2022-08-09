package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class AllTermsIndexedVisitorTest {
    
    private static final Set<String> indexedFields = Sets.newHashSet("FOO", "FOO2", "FOO3");
    private static MockMetadataHelper helper;
    private static ShardQueryConfiguration config;
    
    @BeforeAll
    public static void beforeClass() {
        helper = new MockMetadataHelper();
        helper.setIndexedFields(indexedFields);
        config = new ShardQueryConfiguration();
    }
    
    @Test
    public void testIndexedSingleTerm() throws ParseException {
        String query = "FOO == 'bar'";
        testIsIndexed(query);
    }
    
    @Test
    public void testIndexedConjunction() throws ParseException {
        String query = "FOO == 'bar' || FOO2 == 'bar'";
        testIsIndexed(query);
    }
    
    @Test
    public void testIndexedDisjunction() throws ParseException {
        String query = "FOO == 'bar' && FOO2 == 'bar'";
        testIsIndexed(query);
    }
    
    @Test
    public void testNonIndexedSingleTerm() {
        String query = "BAR == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testNonIndexedConjunction() {
        String query = "BAR == 'foo' && BAR2 == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testNonIndexedDisjunction() {
        String query = "FOO == 'bar' || BAR == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testNonIndexedDoubleDisjunction() {
        String query = "BAR == 'foo' || BAR2 == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testNonIndexedTripleDisjunction() {
        String query = "FOO == 'bar' || BAR == 'foo' || BAR2 == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testFunction() {
        String query = "FOO == 'bar' || content:phrase(termOffsetMap, 'bar', 'too')";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testOnlyFunction() {
        String query = "content:phrase(termOffsetMap, 'bar', 'too')";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testFunctionOverIndexedField() {
        String query = "content:phrase(termOffsetMap, FOO, 'bar', 'too')";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testRegex() {
        String query = "FOO =~ 'bar.*'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testRegexWithExtraTerm() {
        String query = "FOO =~ 'bar.*' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testNegatedRegex() {
        String query = "FOO !~ 'bar.*'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testNegatedRegexWithExtraTerm() {
        String query = "FOO !~ 'bar.*' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testLessThan() {
        String query = "FOO < '+aE1'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testLessThanWithExtraTerm() {
        String query = "FOO < '+aE1' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testLessThanEquals() {
        String query = "FOO <= '+aE1'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testLessThanEqualsWithExtraTerm() {
        String query = "FOO <= '+aE1' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testGreaterThan() {
        String query = "FOO > '+aE1'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testGreaterThanWithExtraTerm() {
        String query = "FOO > '+aE1' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testGreaterThanEquals() {
        String query = "FOO >= '+aE1'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
    }
    
    @Test
    public void testGreaterThanEqualsWithExtraTerm() throws ParseException {
        String query = "FOO >= '+aE1' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testIsIndexed(query));
        testIsIndexed(query);
    }
    
    private void testIsIndexed(String query) throws ParseException {
        ASTJexlScript original = JexlASTHelper.parseJexlQuery(query);
        JexlNode result = AllTermsIndexedVisitor.isIndexed(original, config, helper);
        
        // Verify the resulting script has a valid lineage.
        JexlNodeAssert.assertThat(result).hasValidLineage();
        
        // Verify the original script was not modified and has a valid lineage.
        JexlNodeAssert.assertThat(original).isEqualTo(query).hasValidLineage();
    }
}
