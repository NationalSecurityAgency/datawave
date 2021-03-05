package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

public class SatisfactionVisitorTest {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(SatisfactionVisitorTest.class);
    
    protected Set<String> indexOnlyFields = Sets.newHashSet("IDXONLY");
    
    protected Collection<String> includeReferences = Sets.newHashSet("INCLUDEME", "MAKE", "COLOR", "OWNER", "BBOX_USER");
    protected Collection<String> excludeReferences = Sets.newHashSet("EXCLUDEME");
    
    String[] originalQueries = {"MAKE == 'Ford' && COLOR == 'red' && OWNER != null", "MAKE == 'Ford' && COLOR == 'red' && OWNER == null",
            "MAKE == 'Ford' && COLOR == 'red'", "filter:includeRegex(MAKE, 'vw') && COLOR == 'red'",
            "intersects_bounding_box(BBOX_USER, 50.932610, 51.888420, 35.288080, 35.991210)", "f:between(COLOR, 'red', 'rouge')",
            "MAKE == 'Ford' && EXCLUDEME == 'foo'", "((_Value_ = true) && (FOO_USER >= '09021f44' && FOO_USER <= '09021f47'))",
            "((_List_ = true) && (FOO_USER >= '09021f44' && FOO_USER <= '09021f47'))", "FOO_USER >= '09021f44' && FOO_USER <= '09021f47'",
            "(MAKE == null || ((_Delayed_ = true) && (MAKE == '020')) || MAKE == null)"};
    boolean[] expectedResults = {false, false, true, false, false, true, false, true, true, false, false};
    
    @Test
    public void test() throws Exception {
        
        for (int i = 0; i < originalQueries.length; i++) {
            String query = originalQueries[i];
            workIt(query, expectedResults[i]);
        }
    }
    
    void workIt(String query, boolean expected) throws Exception {
        System.err.println("incoming:" + query);
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        SatisfactionVisitor satisfactionVisitor = new SatisfactionVisitor(indexOnlyFields, includeReferences, excludeReferences, true);
        satisfactionVisitor.visit(script, null);
        Assert.assertEquals(expected, satisfactionVisitor.isQueryFullySatisfied);
    }
    
}
