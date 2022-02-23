package datawave.query.tables.facets;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.EmptyUnfieldedTermExpansionException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MetadataHelper;
import datawave.test.JexlNodeAssert;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

public class FacetCheckTest {
    
    private static final Set<String> indexedFields = Collections.unmodifiableSet(Sets.newHashSet("FOO", "FOO2", "FOO3"));
    
    private FacetCheck facetCheck;
    
    @Before
    public void before() throws TableNotFoundException, IllegalAccessException, InstantiationException {
        Multimap<String,String> facets = HashMultimap.create();
        facets.put("FACET1", "VALUE");
        facets.put("FACET2", "VALUE");
        facets.put("FACET3", "VALUE");
        
        ShardQueryConfiguration shardQueryConfiguration = mock(ShardQueryConfiguration.class);
        FacetedConfiguration facetedConfiguration = mock(FacetedConfiguration.class);
        expect(facetedConfiguration.getFacetMetadataTableName()).andReturn("facetMetadata");
        MetadataHelper metadataHelper = mock(MetadataHelper.class);
        expect(metadataHelper.isIndexed(anyString(), anyObject())).andAnswer(() -> indexedFields.contains(getCurrentArguments()[0]));
        expect(metadataHelper.getFacets("facetMetadata")).andReturn(facets);
        
        replay(shardQueryConfiguration, facetedConfiguration, metadataHelper);
        
        facetCheck = new FacetCheck(shardQueryConfiguration, facetedConfiguration, metadataHelper);
    }
    
    @Test(expected = EmptyUnfieldedTermExpansionException.class)
    public void testAnyFieldSingleTerm() throws ParseException {
        String query = Constants.ANY_FIELD + " == 'bar'";
        testVisitor(query);
    }
    
    @Test(expected = EmptyUnfieldedTermExpansionException.class)
    public void testNoFieldSingleTerm() throws ParseException {
        String query = Constants.NO_FIELD + " == 'bar'";
        testVisitor(query);
    }
    
    @Test
    public void testFacetedSingleTerm() throws ParseException {
        String query = "FACET1 == 'bar'";
        testVisitor(query);
    }
    
    @Test
    public void testFacetedConjunction() throws ParseException {
        String query = "FACET1 == 'bar' || FACET2 == 'bar'";
        testVisitor(query);
    }
    
    @Test
    public void testFacetedDisjunction() throws ParseException {
        String query = "FACET1 == 'bar' && FACET2 == 'bar'";
        testVisitor(query);
    }
    
    @Test(expected = EmptyUnfieldedTermExpansionException.class)
    public void testAnyFieldsTerm() throws ParseException {
        String query = Constants.ANY_FIELD + " == 'bar'";
        testVisitor(query);
    }
    
    @Test(expected = EmptyUnfieldedTermExpansionException.class)
    public void testNoFieldsTerm() throws ParseException {
        String query = Constants.NO_FIELD + " == 'bar'";
        testVisitor(query);
    }
    
    @Test
    public void testAnyFieldAndFacetDisjunction() throws ParseException {
        String query = Constants.ANY_FIELD + " == 'bar' && FACET2 == 'bar'";
        testVisitor(query);
    }
    
    @Test
    public void testNoFieldAndFacetDisjunction() throws ParseException {
        String query = Constants.NO_FIELD + " == 'bar' && FACET2 == 'bar'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonFacetedSingleTerm() throws ParseException {
        String query = "BAR == 'foo'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonFacetedConjunction() throws ParseException {
        String query = "BAR == 'foo' && BAR2 == 'foo'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonFacetedDisjunction() throws ParseException {
        String query = "FOO == 'bar' || BAR == 'foo'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonFacetedDoubleDisjunction() throws ParseException {
        String query = "BAR == 'foo' || BAR2 == 'foo'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNonFacetedTripleDisjunction() throws ParseException {
        String query = "FOO == 'bar' || BAR == 'foo' || BAR2 == 'foo'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testFunction() throws ParseException {
        String query = "FOO == 'bar' || content:phrase(termOffsetMap, 'bar', 'too')";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testOnlyFunction() throws ParseException {
        String query = "content:phrase(termOffsetMap, 'bar', 'too')";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testFunctionOverIndexedField() throws ParseException {
        String query = "content:phrase(termOffsetMap, FOO, 'bar', 'too')";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testRegex() throws ParseException {
        String query = "FOO =~ 'bar.*'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testRegexWithExtraTerm() throws ParseException {
        String query = "FOO =~ 'bar.*' || FOO == 'bar'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNegatedRegex() throws ParseException {
        String query = "FOO !~ 'bar.*'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testNegatedRegexWithExtraTerm() throws ParseException {
        String query = "FOO !~ 'bar.*' || FOO == 'bar'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testLessThan() throws ParseException {
        String query = "FOO < '+aE1'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testLessThanWithExtraTerm() throws ParseException {
        String query = "FOO < '+aE1' || FOO == 'bar'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testLessThanEquals() throws ParseException {
        String query = "FOO <= '+aE1'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testLessThanEqualsWithExtraTerm() throws ParseException {
        String query = "FOO <= '+aE1' || FOO == 'bar'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testGreaterThan() throws ParseException {
        String query = "FOO > '+aE1'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testGreaterThanWithExtraTerm() throws ParseException {
        String query = "FOO > '+aE1' || FOO == 'bar'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testGreaterThanEquals() throws ParseException {
        String query = "FOO >= '+aE1'";
        testVisitor(query);
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testGreaterThanEqualsWithExtraTerm() throws ParseException {
        String query = "FOO >= '+aE1' || FOO == 'bar'";
        testVisitor(query);
    }
    
    private void testVisitor(String query) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        JexlNode result = (JexlNode) script.jjtAccept(facetCheck, null);
        
        // Verify the result script has a valid lineage.
        JexlNodeAssert.assertThat(result).hasValidLineage();
        
        // Verify the original script was not modified, and has a valid lineage.
        JexlNodeAssert.assertThat(script).isEqualTo(query).hasValidLineage();
    }
}
