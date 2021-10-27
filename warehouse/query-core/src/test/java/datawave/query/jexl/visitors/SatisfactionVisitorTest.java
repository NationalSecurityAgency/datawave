package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SatisfactionVisitorTest {
    
    private static final Set<String> indexOnlyFields = Collections.singleton("IDXONLY");
    private static final Set<String> includeReferences = Collections.unmodifiableSet(Sets.newHashSet("INCLUDEME", "MAKE", "COLOR", "OWNER", "BBOX_USER"));
    private static final Set<String> excludeReferences = Collections.singleton("EXCLUDEME");
    
    @Test
    public void test() throws Exception {
        assertFalse(isQueryFullySatisfied("MAKE == 'Ford' && COLOR == 'red' && OWNER != null"));
        assertFalse(isQueryFullySatisfied("MAKE == 'Ford' && COLOR == 'red' && OWNER == null"));
        assertFalse(isQueryFullySatisfied("filter:includeRegex(MAKE, 'vw') && COLOR == 'red'"));
        assertFalse(isQueryFullySatisfied("intersects_bounding_box(BBOX_USER, 50.932610, 51.888420, 35.288080, 35.991210)"));
        assertFalse(isQueryFullySatisfied("MAKE == 'Ford' && EXCLUDEME == 'foo'"));
        assertFalse(isQueryFullySatisfied("FOO_USER >= '09021f44' && FOO_USER <= '09021f47'"));
        assertFalse(isQueryFullySatisfied("(MAKE == null || ((_Delayed_ = true) && (MAKE == '020')) || MAKE == null)"));
        
        assertTrue(isQueryFullySatisfied("MAKE == 'Ford' && COLOR == 'red'"));
        assertTrue(isQueryFullySatisfied("f:between(COLOR, 'red', 'rouge')"));
        assertTrue(isQueryFullySatisfied("((_Value_ = true) && (FOO_USER >= '09021f44' && FOO_USER <= '09021f47'))"));
        assertTrue(isQueryFullySatisfied("((_List_ = true) && (FOO_USER >= '09021f44' && FOO_USER <= '09021f47'))"));
    }
    
    private boolean isQueryFullySatisfied(String query) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        SatisfactionVisitor visitor = new SatisfactionVisitor(indexOnlyFields, includeReferences, excludeReferences, true);
        visitor.visit(script, null);
        return visitor.isQueryFullySatisfied;
    }
}
