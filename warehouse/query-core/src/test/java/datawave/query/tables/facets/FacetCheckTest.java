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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.getCurrentArguments;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FacetCheckTest {
    
    private static final Set<String> indexedFields = Collections.unmodifiableSet(Sets.newHashSet("FOO", "FOO2", "FOO3"));
    
    private FacetCheck facetCheck;
    
    @BeforeEach
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
    
    @Test
    public void testAnyFieldSingleTerm() {
        String query = Constants.ANY_FIELD + " == 'bar'";
        assertThrows(EmptyUnfieldedTermExpansionException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testNoFieldSingleTerm() {
        String query = Constants.NO_FIELD + " == 'bar'";
        assertThrows(EmptyUnfieldedTermExpansionException.class, () -> testVisitor(query));
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
    
    @Test
    public void testAnyFieldsTerm() {
        String query = Constants.ANY_FIELD + " == 'bar'";
        assertThrows(EmptyUnfieldedTermExpansionException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testNoFieldsTerm() {
        String query = Constants.NO_FIELD + " == 'bar'";
        assertThrows(EmptyUnfieldedTermExpansionException.class, () -> testVisitor(query));
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
    
    @Test
    public void testNonFacetedSingleTerm() {
        String query = "BAR == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testNonFacetedConjunction() {
        String query = "BAR == 'foo' && BAR2 == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testNonFacetedDisjunction() {
        String query = "FOO == 'bar' || BAR == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testNonFacetedDoubleDisjunction() {
        String query = "BAR == 'foo' || BAR2 == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testNonFacetedTripleDisjunction() {
        String query = "FOO == 'bar' || BAR == 'foo' || BAR2 == 'foo'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testFunction() {
        String query = "FOO == 'bar' || content:phrase(termOffsetMap, 'bar', 'too')";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
        
    }
    
    @Test
    public void testOnlyFunction() {
        String query = "content:phrase(termOffsetMap, 'bar', 'too')";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testFunctionOverIndexedField() {
        String query = "content:phrase(termOffsetMap, FOO, 'bar', 'too')";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testRegex() {
        String query = "FOO =~ 'bar.*'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testRegexWithExtraTerm() {
        String query = "FOO =~ 'bar.*' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testNegatedRegex() {
        String query = "FOO !~ 'bar.*'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testNegatedRegexWithExtraTerm() {
        String query = "FOO !~ 'bar.*' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testLessThan() {
        String query = "FOO < '+aE1'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testLessThanWithExtraTerm() {
        String query = "FOO < '+aE1' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testLessThanEquals() {
        String query = "FOO <= '+aE1'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testLessThanEqualsWithExtraTerm() {
        String query = "FOO <= '+aE1' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testGreaterThan() {
        String query = "FOO > '+aE1'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testGreaterThanWithExtraTerm() {
        String query = "FOO > '+aE1' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testGreaterThanEquals() {
        String query = "FOO >= '+aE1'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
    }
    
    @Test
    public void testGreaterThanEqualsWithExtraTerm() {
        String query = "FOO >= '+aE1' || FOO == 'bar'";
        assertThrows(DatawaveFatalQueryException.class, () -> testVisitor(query));
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
