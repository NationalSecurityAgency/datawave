package datawave.query.jexl;

import com.google.common.collect.Sets;
import datawave.query.jexl.lookups.IndexLookupMap;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JexlNodeFactoryTest {
    
    @Test
    public void testNoFieldExpansion() throws ParseException {
        IndexLookupMap indexLookupMap = new IndexLookupMap(10, 10);
        indexLookupMap.putAll("FOO", Sets.newHashSet("6", "9"));
        indexLookupMap.putAll("BAR", Sets.newHashSet("7", "8"));
        
        JexlNode originalNode = JexlASTHelper.parseJexlQuery("((_Bounded_ = true) && (FOO > 5 && FOO < 10))");
        JexlNode node = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, false, originalNode, indexLookupMap, false,
                        true, false);
        
        assertEquals("(FOO == '6' || FOO == '7' || FOO == '8' || FOO == '9')", JexlStringBuildingVisitor.buildQuery(node));
        assertTrue(JexlASTHelper.validateLineage(node, false));
    }
}
