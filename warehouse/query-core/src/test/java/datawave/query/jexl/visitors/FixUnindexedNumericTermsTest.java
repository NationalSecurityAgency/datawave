package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.GeoLatType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FixUnindexedNumericTermsTest {
    
    private ShardQueryConfiguration config;
    
    @Before
    public void setUp() throws Exception {
        config = new ShardQueryConfiguration();
        Multimap<String,Type<?>> queryFieldsDataTypes = HashMultimap.create();
        queryFieldsDataTypes.put("INDEXED", new GeoLatType());
        config.setQueryFieldsDatatypes(queryFieldsDataTypes);
    }
    
    @Test
    public void testUnindexedStringLiteral() throws ParseException {
        String original = "FOO == '-1'";
        String expected = "FOO == -1";
        assertResult(original, expected);
    }
    
    @Test
    public void testMultipleUnindexedStringLiterals() throws ParseException {
        String original = "BAR == '-2' && FOO == '-1'";
        String expected = "BAR == -2 && FOO == -1";
        assertResult(original, expected);
    }
    
    @Test
    public void testIndexedStringLiteral() throws ParseException {
        String original = "INDEXED == '-1'";
        assertResult(original, original);
    }
    
    @Test
    public void testMultipleIndexedStringLiterals() throws ParseException {
        String original = "INDEXED == '-2' && INDEXED == '-1'";
        assertResult(original, original);
    }
    
    @Test
    public void testIndexedAndUnindexedCombination() throws ParseException {
        String original = "BAR == '-2' && INDEXED == '-1'";
        String expected = "BAR == -2 && INDEXED == '-1'";
        assertResult(original, expected);
    }
    
    private void assertResult(String original, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(original);
        ASTJexlScript result = FixUnindexedNumericTerms.fixNumerics(config, script);
        
        assertEquals(expected, JexlStringBuildingVisitor.buildQuery(result));
        assertTrue(JexlASTHelper.validateLineage(result, true));
    }
}
