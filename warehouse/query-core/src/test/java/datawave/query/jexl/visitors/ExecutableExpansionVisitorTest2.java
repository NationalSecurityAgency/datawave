package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test just the visitor with input/output query plans
 */
public class ExecutableExpansionVisitorTest2 {
    
    private final Set<String> indexedFields = Sets.newHashSet("CITY", "STATE");
    
    private ShardQueryConfiguration config;
    private MockMetadataHelper metadataHelper;
    
    @Before
    public void beforeEach() {
        config = new ShardQueryConfiguration();
        config.setIndexedFields(indexedFields);
        
        metadataHelper = new MockMetadataHelper();
        metadataHelper.setIndexedFields(indexedFields);
    }
    
    // CITY and STATE are indexed, CITY gets distributed
    @Test
    public void testExecutableRedistribution() {
        String query = "CITY == 'london' && (CODE == 'ita' || STATE == 'missouri')";
        String expected = "(STATE == 'missouri' && CITY == 'london') || (CODE == 'ita' && CITY == 'london')";
        test(query, expected, config, metadataHelper);
    }
    
    private void test(String query, String expected, ShardQueryConfiguration config, MetadataHelper helper) {
        try {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            
            ASTJexlScript expanded = ExecutableExpansionVisitor.expand(script, config, helper);
            
            // validate lineage
            assertTrue(JexlASTHelper.validateLineage(expanded, false));
            
            // validate built query string
            String postVisit = JexlStringBuildingVisitor.buildQuery(expanded);
            assertEquals(expected, postVisit);
            
        } catch (ParseException e) {
            e.printStackTrace();
            fail("problem while executing visitor");
        }
    }
    
}
