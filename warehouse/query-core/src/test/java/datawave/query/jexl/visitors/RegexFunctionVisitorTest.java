package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

import static org.junit.Assert.assertTrue;

public class RegexFunctionVisitorTest {
    
    private static final Logger log = Logger.getLogger(RegexFunctionVisitorTest.class);
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    @Test
    public void testDoubleEndedWildCard() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDA,'.*all_.*')";
        String expected = "_ANYFIELD_ == 'email' && ANOTHER_FIELD == 'blah' && filter:includeRegex(FIELDA, '.*all_.*')";
        String field = "FIELDA";
        
        assertVisitorResult(query, expected, field);
    }
    
    @Test
    public void testDoubleEndedWildCard2() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDA,'?.*all_.*?')";
        String expected = "_ANYFIELD_ == 'email' && ANOTHER_FIELD == 'blah' && filter:includeRegex(FIELDA, '?.*all_.*?')";
        String field = "FIELDA";
    
        assertVisitorResult(query, expected, field);
    }
    
    @Test
    public void testEndWildcard() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDA,'all_.*?')";
        String expected = "_ANYFIELD_ == 'email' && ANOTHER_FIELD == 'blah' && FIELDA =~ 'all_.*?'";
        String field = "FIELDA";
    
        assertVisitorResult(query, expected, field);
    }
    
    @Test
    public void testEndWildCardNotIndexOnly() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDB, 'all_.*?')";
        String expected = "_ANYFIELD_ == 'email' && ANOTHER_FIELD == 'blah' && filter:includeRegex(FIELDB, 'all_.*?')";
        String field = "FIELDA";
    
        assertVisitorResult(query, expected, field);
    }
    
    @Test
    public void testBadRegex() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDA, '(?#icu)Friendly')";
        String field = "FIELDA";
    
        exception.expect(DatawaveFatalQueryException.class);
        assertVisitorResult(query, query, field);
    }
    
    @Test
    public void testMixedEventNonEvent() throws ParseException {
        String query = "filter:includeRegex(EVENT_FIELD || NON_EVENT_FIELD,'all_.*?')";
        String field = "NON_EVENT_FIELD";
    
        assertVisitorResult(query, query, field);
    }
    
    private void assertVisitorResult(String original, String expected, String field) throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(original);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
    
        Set<String> indexOnlyFields = Sets.newHashSet(field);
    
        JexlNode actual = RegexFunctionVisitor.expandRegex(null, null, indexOnlyFields, originalScript);
        
        assertScriptEquality(actual, expectedScript);
        assertTrue(JexlASTHelper.validateLineage(actual, true));
    }
    
    private void assertScriptEquality(JexlNode actual, ASTJexlScript expectedScript) throws ParseException {
        ASTJexlScript actualScript = JexlASTHelper.parseJexlQuery(JexlStringBuildingVisitor.buildQuery(actual));
        TreeEqualityVisitor.Reason reason = new TreeEqualityVisitor.Reason();
        boolean equal = TreeEqualityVisitor.isEqual(expectedScript, actualScript, reason);
        if (!equal) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actualScript));
        }
        assertTrue(reason.reason, equal);
    }
}
