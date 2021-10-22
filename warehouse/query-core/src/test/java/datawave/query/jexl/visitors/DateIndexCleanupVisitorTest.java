package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Test;

import static datawave.query.Constants.SHARD_DAY_HINT;

public class DateIndexCleanupVisitorTest {
    
    @Test
    public void testConjunction() throws ParseException {
        String original = "FOO == 'bar' && (" + SHARD_DAY_HINT + " = 'hello,world')";
        String expected = "FOO == 'bar'";
        testCleanup(original, expected);
    }
    
    @Test
    public void testDisjunction() throws ParseException {
        String original = "FOO == 'bar' || (" + SHARD_DAY_HINT + " = 'hello,world')";
        String expected = "FOO == 'bar'";
        testCleanup(original, expected);
    }
    
    @Test
    public void testDuplicateHints() throws ParseException {
        String original = "FOO == 'bar' || (" + SHARD_DAY_HINT + " = 'hello,world' && " + SHARD_DAY_HINT + " = 'hello,world')";
        String expected = "FOO == 'bar'";
        testCleanup(original, expected);
    }
    
    @Test
    public void testConjunctionHint() throws ParseException {
        String original = "((FOO == 'bar' || (" + SHARD_DAY_HINT + " = 'hello,world')) && (" + SHARD_DAY_HINT + " = 'hello,world'))";
        String expected = "(FOO == 'bar')";
        testCleanup(original, expected);
    }
    
    @Test
    public void testNegatedHint() throws ParseException {
        String original = "(FOO == 'bar' && (!(filter:betweenDates(SOME_DATE, 'beginDate', 'endDate')) || !((SHARDS_AND_DAYS = '1111,2222'))))";
        String expected = "(FOO == 'bar' && (!(filter:betweenDates(SOME_DATE, 'beginDate', 'endDate'))))";
        testCleanup(original, expected);
    }
    
    @Test
    public void testOnlyHint() throws ParseException {
        String original = "(" + SHARD_DAY_HINT + " = 'hello,world')";
        String expected = "";
        testCleanup(original, expected);
    }
    
    private void testCleanup(String original, String expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(original);
        
        ASTJexlScript cleaned = DateIndexCleanupVisitor.cleanup(script);
        
        JexlNodeAssert.assertThat(cleaned).isEqualTo(expected).hasValidLineage();
        JexlNodeAssert.assertThat(script).isEqualTo(original).hasValidLineage();
    }
}
