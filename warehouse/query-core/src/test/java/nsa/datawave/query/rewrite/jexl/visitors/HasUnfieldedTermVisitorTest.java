package nsa.datawave.query.rewrite.jexl.visitors;

import nsa.datawave.query.language.parser.jexl.JexlNode;
import nsa.datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class HasUnfieldedTermVisitorTest {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(HasUnfieldedTermVisitorTest.class);
    
    String[] originalQueries = { //
    
    "_ANYFIELD_ == 'FOO'", //
            "AG.max() == 40", //
            "BIRTH_DATE.min() < '1920-12-28T00:00:05.000Z'", //
            "FOO == 'bar'"};
    boolean[] expectedResults = {true, false, false, false};
    
    @Test
    public void test() throws Exception {
        
        for (int i = 0; i < originalQueries.length; i++) {
            String query = originalQueries[i];
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            
            Assert.assertEquals("query:" + query, expectedResults[i],
                            ((AtomicBoolean) new JexlASTHelper.HasUnfieldedTermVisitor().visit(script, new AtomicBoolean(false))).get());
        }
    }
    
    @Test
    public void testLucene() throws Exception {
        
        String[] originalQueries = { //
        
        "FOO", //
                "FOO || BAR || AG:40", //
                "BIRTH_DATE:1920", //
                "FOO:bar"};
        boolean[] expectedResults = {true, true, false, false};
        for (int i = 0; i < originalQueries.length; i++) {
            String query = originalQueries[i];
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(new LuceneToJexlQueryParser().convertToJexlNode(query).toString());
            // JexlASTHelper.parseJexlQuery(query);
            
            Assert.assertEquals("query:" + query, expectedResults[i],
                            ((AtomicBoolean) new JexlASTHelper.HasUnfieldedTermVisitor().visit(script, new AtomicBoolean(false))).get());
        }
    }
    
}
