package datawave.query.rewrite.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.rewrite.jexl.JexlASTHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class HasMethodVisitorTest {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(HasMethodVisitorTest.class);
    
    String[] originalQueries = { //
    
    "FOO.size() > 0", //
            "AG.max() == 40", //
            "BIRTH_DATE.min() < '1920-12-28T00:00:05.000Z'", //
            "FOO == 'bar'"};
    boolean[] expectedResults = {true, true, true, false};
    
    @Test
    public void test() throws Exception {
        
        for (int i = 0; i < originalQueries.length; i++) {
            String query = originalQueries[i];
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            
            Assert.assertEquals("query:" + query, expectedResults[i],
                            ((AtomicBoolean) new JexlASTHelper.HasMethodVisitor().visit(script, new AtomicBoolean(false))).get());
        }
    }
}
