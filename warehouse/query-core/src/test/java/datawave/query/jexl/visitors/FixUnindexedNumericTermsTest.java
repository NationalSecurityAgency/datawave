package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.IdentityDataType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertTrue;

public class FixUnindexedNumericTermsTest {
    
    private static final Logger log = Logger.getLogger(FixUnindexedNumericTermsTest.class);
    
    private Multimap<String,Type<?>> datatypes;
    
    private ShardQueryConfiguration config;
    
    @Before
    public void setUp() throws Exception {
        datatypes = HashMultimap.create();
        config = EasyMock.mock(ShardQueryConfiguration.class);
        expect(config.getQueryFieldsDatatypes()).andReturn(datatypes).anyTimes();
        replay(config);
    }
    
    @Test
    public void testNullValues() throws ParseException {
        assertResult("FOO == null", "FOO == null");
        assertResult("FOO == 'null'", "FOO == 'null'");
    }
    
    @Test
    public void testEQNode() throws ParseException {
        givenIndexedField("INDEXED_A");
        givenIndexedField("INDEXED_B");
        
        assertResult("FOO == '10' && BAR == 2 && INDEXED_A == '10' && INDEXED_B == 2", "FOO == 10 && BAR == 2 && INDEXED_A == '10' && INDEXED_B == 2");
    }
    
    @Test
    public void testNENode() throws ParseException {
        givenIndexedField("INDEXED_A");
        givenIndexedField("INDEXED_B");
        
        assertResult("FOO != '10' && BAR != 2 && INDEXED_A != '10' && INDEXED_B != 2", "FOO != 10 && BAR != 2 && INDEXED_A != '10' && INDEXED_B != 2");
    }
    
    @Test
    public void testERNode() throws ParseException {
        givenIndexedField("INDEXED_A");
        givenIndexedField("INDEXED_B");
        
        assertResult("FOO =~ '10' && BAR =~ 2 && INDEXED_A =~ '10' && INDEXED_B =~ 2", "FOO =~ 10 && BAR =~ 2 && INDEXED_A =~ '10' && INDEXED_B =~ 2");
    }
    
    @Test
    public void testNRNode() throws ParseException {
        givenIndexedField("INDEXED_A");
        givenIndexedField("INDEXED_B");
        
        assertResult("FOO !~ '10' && BAR !~ 2 && INDEXED_A !~ '10' && INDEXED_B !~ 2", "FOO !~ 10 && BAR !~ 2 && INDEXED_A !~ '10' && INDEXED_B !~ 2");
    }
    
    @Test
    public void testLTNode() throws ParseException {
        givenIndexedField("INDEXED_A");
        givenIndexedField("INDEXED_B");
        
        assertResult("FOO < '10' && BAR < 2 && INDEXED_A < '10' && INDEXED_B < 2", "FOO < 10 && BAR < 2 && INDEXED_A < '10' && INDEXED_B < 2");
    }
    
    @Test
    public void testLENode() throws ParseException {
        givenIndexedField("INDEXED_A");
        givenIndexedField("INDEXED_B");
        
        assertResult("FOO <= '10' && BAR <= 2 && INDEXED_A <= '10' && INDEXED_B <= 2", "FOO <= 10 && BAR <= 2 && INDEXED_A <= '10' && INDEXED_B <= 2");
    }
    
    @Test
    public void testGTNode() throws ParseException {
        givenIndexedField("INDEXED_A");
        givenIndexedField("INDEXED_B");
        
        assertResult("FOO > '10' && BAR > 2 && INDEXED_A > '10' && INDEXED_B > 2", "FOO > 10 && BAR > 2 && INDEXED_A > '10' && INDEXED_B > 2");
    }
    
    @Test
    public void testGENode() throws ParseException {
        givenIndexedField("INDEXED_A");
        givenIndexedField("INDEXED_B");
        
        assertResult("FOO >= '10' && BAR >= 2 && INDEXED_A >= '10' && INDEXED_B >= 2", "FOO >= 10 && BAR >= 2 && INDEXED_A >= '10' && INDEXED_B >= 2");
    }
    
    private void givenIndexedField(String field) {
        datatypes.put(field, new IdentityDataType());
    }
    
    private void assertResult(String original, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        ASTJexlScript actual = FixUnindexedNumericTerms.fixNumerics(config, originalScript);
        
        assertScriptEquality(actual, expected);
        assertLineage(actual);
        
        assertScriptEquality(originalScript, original);
        assertLineage(originalScript);
        
        assertTrue(TreeEqualityVisitor.isEqual(expectedScript, actual));
    }
    
    private void assertScriptEquality(ASTJexlScript actual, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actual);
        if (!comparison.isEqual()) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actual));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
