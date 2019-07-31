package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.LcType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class FetchDataTypesVisitorTest {
    
    private static ShardQueryConfiguration config = new ShardQueryConfiguration();
    private static MockMetadataHelper helper = new MockMetadataHelper();
    
    private Set<String> dataTypeFilter = Collections.singleton("datatype1");
    
    @BeforeClass
    public static void setup() {
        // 1. Configure the ShardQueryConfig
        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));
        
        // 2. Configure the MockMetadataHelper
        helper.addNormalizers("FOO", Sets.newHashSet(new LcNoDiacriticsType()));
        helper.addNormalizers("FOO2", Sets.newHashSet(new LcType()));
        helper.addNormalizers("FOO3", Sets.newHashSet(new NumberType()));
        helper.addNormalizers("FOO4", Sets.newHashSet(new LcType(), new LcNoDiacriticsType()));
    }
    
    @Test
    public void testSingleTermNormalizer() throws ParseException {
        String query = "FOO == 'bar'";
        
        Multimap<String,Type<?>> expected = HashMultimap.create();
        expected.put("FOO", new LcNoDiacriticsType());
        
        runTest(query, expected);
    }
    
    @Test
    public void testTwoSingleTermNormalizers() throws ParseException {
        String query = "FOO2 == 'bar' && FOO3 == '3'";
        
        Multimap<String,Type<?>> expected = HashMultimap.create();
        expected.put("FOO2", new LcType());
        expected.put("FOO3", new NumberType());
        
        runTest(query, expected);
    }
    
    @Test
    public void testSingleTermTwoNormalizers() throws ParseException {
        String query = "FOO4 == 'bar'";
        
        Multimap<String,Type<?>> expected = HashMultimap.create();
        expected.put("FOO4", new LcType());
        expected.put("FOO4", new LcNoDiacriticsType());
        
        runTest(query, expected);
    }
    
    @Test
    public void testFunctionWithNormalizer() throws ParseException {
        String query = "content:phrase(FOO2, termOffsetMap, 'bar', 'baz')";
        
        Multimap<String,Type<?>> expected = HashMultimap.create();
        expected.put("FOO2", new LcType());
        
        runTest(query, expected);
    }
    
    @Test
    public void testRegexWithNormalizer() throws ParseException {
        String query = "filter:includeRegex(FOO2, 'bar.*')";
        
        Multimap<String,Type<?>> expected = HashMultimap.create();
        expected.put("FOO2", new LcType());
        
        runTest(query, expected);
    }
    
    private void runTest(String query, Multimap<String,Type<?>> expected) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        Multimap<String,Type<?>> dataTypes = FetchDataTypesVisitor.fetchDataTypes(helper, dataTypeFilter, script);
        
        assertEquals(expected, dataTypes);
    }
}
