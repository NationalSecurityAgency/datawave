package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HasUnfieldedTermVisitorTest {
    
    private ASTJexlScript script;
    
    @Test
    public void test() throws Exception {
        givenJexlQuery("_ANYFIELD_ == 'FOO'");
        assertTrue(hasUnfieldedTermVisitor());
        
        givenJexlQuery("AG.max() == 40");
        assertFalse(hasUnfieldedTermVisitor());
        
        givenJexlQuery("BIRTH_DATE.min() < '1920-12-28T00:00:05.000Z'");
        assertFalse(hasUnfieldedTermVisitor());
        
        givenJexlQuery("FOO == 'bar'");
        assertFalse(hasUnfieldedTermVisitor());
    }
    
    @Test
    public void testLucene() throws Exception {
        givenLuceneQuery("FOO");
        assertTrue(hasUnfieldedTermVisitor());
        
        givenLuceneQuery("FOO || BAR || AG:40");
        assertTrue(hasUnfieldedTermVisitor());
        
        givenLuceneQuery("BIRTH_DATE:1920");
        assertFalse(hasUnfieldedTermVisitor());
        
        givenLuceneQuery("FOO:bar");
        assertFalse(hasUnfieldedTermVisitor());
    }
    
    private void givenJexlQuery(String query) throws ParseException {
        script = JexlASTHelper.parseJexlQuery(query);
    }
    
    private void givenLuceneQuery(String query) throws datawave.query.language.parser.ParseException, ParseException {
        script = JexlASTHelper.parseJexlQuery(new LuceneToJexlQueryParser().convertToJexlNode(query).toString());
    }
    
    private boolean hasUnfieldedTermVisitor() {
        return ((AtomicBoolean) new JexlASTHelper.HasUnfieldedTermVisitor().visit(script, new AtomicBoolean(false))).get();
    }
}
