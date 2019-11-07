package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.exceptions.DatawaveFatalQueryException;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

public class RegexFunctionVisitorTest {
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    @Test
    public void testDoubleEndedWildCard() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDA,'.*all_.*')";
        
        Set<String> indexOnlyFields = Sets.newHashSet();
        indexOnlyFields.add("FIELDA");
        
        JexlNode result = RegexFunctionVisitor.expandRegex(null, null, indexOnlyFields, JexlASTHelper.parseJexlQuery(query));
        
        Assert.assertEquals("_ANYFIELD_ == 'email' && ANOTHER_FIELD == 'blah' && filter:includeRegex(FIELDA, '.*all_.*')",
                        JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testDoubleEndedWildCard2() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDA,'?.*all_.*?')";
        
        Set<String> indexOnlyFields = Sets.newHashSet();
        indexOnlyFields.add("FIELDA");
        
        JexlNode result = RegexFunctionVisitor.expandRegex(null, null, indexOnlyFields, JexlASTHelper.parseJexlQuery(query));
        
        Assert.assertEquals("_ANYFIELD_ == 'email' && ANOTHER_FIELD == 'blah' && filter:includeRegex(FIELDA, '?.*all_.*?')",
                        JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testEndWildcard() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDA,'all_.*?')";
        
        Set<String> indexOnlyFields = Sets.newHashSet();
        indexOnlyFields.add("FIELDA");
        
        JexlNode result = RegexFunctionVisitor.expandRegex(null, null, indexOnlyFields, JexlASTHelper.parseJexlQuery(query));
        
        Assert.assertEquals("_ANYFIELD_ == 'email' && ANOTHER_FIELD == 'blah' && FIELDA =~ 'all_.*?'", JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testEndWildCardNotIndexOnly() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDB, 'all_.*?')";
        
        Set<String> indexOnlyFields = Sets.newHashSet();
        indexOnlyFields.add("FIELDA");
        
        JexlNode result = RegexFunctionVisitor.expandRegex(null, null, indexOnlyFields, JexlASTHelper.parseJexlQuery(query));
        
        Assert.assertEquals("_ANYFIELD_ == 'email' && ANOTHER_FIELD == 'blah' && filter:includeRegex(FIELDB, 'all_.*?')",
                        JexlStringBuildingVisitor.buildQuery(result));
    }
    
    @Test
    public void testBadRegex() throws ParseException {
        String query = "_ANYFIELD_=='email' and ANOTHER_FIELD=='blah' and filter:includeRegex(FIELDA, '(?#icu)Friendly')";
        
        Set<String> indexOnlyFields = Sets.newHashSet();
        indexOnlyFields.add("FIELDA");
        
        exception.expect(DatawaveFatalQueryException.class);
        JexlNode result = RegexFunctionVisitor.expandRegex(null, null, indexOnlyFields, JexlASTHelper.parseJexlQuery(query));
    }
}
