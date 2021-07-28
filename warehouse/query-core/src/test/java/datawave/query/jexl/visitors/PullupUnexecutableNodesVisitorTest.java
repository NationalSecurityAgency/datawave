package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class PullupUnexecutableNodesVisitorTest {
    
    private Set<String> indexedFields;
    private Set<String> indexOnlyFields;
    private Set<String> nonEventFields;
    private MockMetadataHelper metadataHelper;
    private ShardQueryConfiguration config;
    
    @Before
    public void before() {
        indexedFields = Sets.newHashSet("FOO");
        indexOnlyFields = Sets.newHashSet();
        nonEventFields = Sets.newHashSet();
        
        metadataHelper = new MockMetadataHelper();
        metadataHelper.setIndexedFields(indexOnlyFields);
        metadataHelper.setIndexOnlyFields(indexOnlyFields);
        metadataHelper.setNonEventFields(nonEventFields);
        
        config = new ShardQueryConfiguration();
        config.setIndexedFields(indexedFields);
    }
    
    @Test
    public void testPullupDelayed() throws ParseException {
        String query = "!((_Delayed_ = true) && (FOO =~ 'bar.*'))";
        String expected = "!(FOO =~ 'bar.*')";
        test(query, expected);
    }
    
    @Test
    public void testPullUpMultipleDelayed() throws ParseException {
        String query = "((_Delayed_ = true) && ((_Delayed_ = true) && (FOO == 'bar')))";
        String expected = "(FOO == 'bar')";
        test(query, expected);
    }
    
    private void test(String query, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        ASTJexlScript visitedScript = (ASTJexlScript) PullupUnexecutableNodesVisitor.pullupDelayedPredicates(script, true, config, indexedFields,
                        indexOnlyFields, nonEventFields, metadataHelper);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        // assertTrue(TreeEqualityVisitor.isEqual(expectedScript, visitedScript));
        
        String visitedQueryString = JexlStringBuildingVisitor.buildQueryWithoutParse(visitedScript);
        assertEquals(expected, visitedQueryString);
    }
}
