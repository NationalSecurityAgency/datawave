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
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

public class AllTermsIndexedVisitorTest {
    
    private static final Set<String> indexedFields = Sets.newHashSet("FOO", "FOO2", "FOO3");
    private static MockMetadataHelper helper;
    private static ShardQueryConfiguration config;
    
    @BeforeClass
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
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonIndexedSingleTerm() throws ParseException {
        String query = "BAR == 'foo'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonIndexedConjunction() throws ParseException {
        String query = "BAR == 'foo' && BAR2 == 'foo'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonIndexedDisjunction() throws ParseException {
        String query = "FOO == 'bar' || BAR == 'foo'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonIndexedDoubleDisjunction() throws ParseException {
        String query = "BAR == 'foo' || BAR2 == 'foo'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonIndexedTripleDisjunction() throws ParseException {
        String query = "FOO == 'bar' || BAR == 'foo' || BAR2 == 'foo'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testFunction() throws ParseException {
        String query = "FOO == 'bar' || content:phrase(termOffsetMap, 'bar', 'too')";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testOnlyFunction() throws ParseException {
        String query = "content:phrase(termOffsetMap, 'bar', 'too')";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testFunctionOverIndexedField() throws ParseException {
        String query = "content:phrase(termOffsetMap, FOO, 'bar', 'too')";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testRegex() throws ParseException {
        String query = "FOO =~ 'bar.*'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testRegexWithExtraTerm() throws ParseException {
        String query = "FOO =~ 'bar.*' || FOO == 'bar'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNegatedRegex() throws ParseException {
        String query = "FOO !~ 'bar.*'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNegatedRegexWithExtraTerm() throws ParseException {
        String query = "FOO !~ 'bar.*' || FOO == 'bar'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testLessThan() throws ParseException {
        String query = "FOO < '+aE1'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testLessThanWithExtraTerm() throws ParseException {
        String query = "FOO < '+aE1' || FOO == 'bar'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testLessThanEquals() throws ParseException {
        String query = "FOO <= '+aE1'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testLessThanEqualsWithExtraTerm() throws ParseException {
        String query = "FOO <= '+aE1' || FOO == 'bar'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testGreaterThan() throws ParseException {
        String query = "FOO > '+aE1'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testGreaterThanWithExtraTerm() throws ParseException {
        String query = "FOO > '+aE1' || FOO == 'bar'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testGreaterThanEquals() throws ParseException {
        String query = "FOO >= '+aE1'";
        testIsIndexed(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testGreaterThanEqualsWithExtraTerm() throws ParseException {
        String query = "FOO >= '+aE1' || FOO == 'bar'";
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
