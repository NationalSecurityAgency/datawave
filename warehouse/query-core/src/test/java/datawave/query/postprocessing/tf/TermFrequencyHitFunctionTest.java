package datawave.query.postprocessing.tf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class TermFrequencyHitFunctionTest {
    
    // phrase functions
    
    @Test
    public void test_phraseFunction_eventQuery_includeOnly() throws ParseException {
        String query = "content:phrase(TEXT, termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    @Test
    public void test_phraseFunction_tldQuery_includeOnly() throws ParseException {
        String query = "content:phrase(TEXT, termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void test_phraseFunction_eventQuery_includeExclude() throws ParseException {
        String query = "(content:phrase(TEXT, termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house') && "
                        + "((_Delayed_ = true) && (content:phrase(TEXT, termOffsetMap, 'purple', 'pumpkin') && TEXT == 'purple' && TEXT == 'pumpkin'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0purple\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0pumpkin\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    @Test
    public void test_phraseFunction_tldQuery_includeExclude() throws ParseException {
        String query = "(content:phrase(TEXT, termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house') && "
                        + "((_Delayed_ = true) && (content:phrase(TEXT, termOffsetMap, 'purple', 'pumpkin') && TEXT == 'purple' && TEXT == 'pumpkin'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0purple\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0pumpkin\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void test_phraseFunction_tldQuery_includeExclude_differentDocs() throws ParseException {
        String query = "(content:phrase(TEXT, termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house') && "
                        + "((_Delayed_ = true) && (content:phrase(TEXT, termOffsetMap, 'orange', 'oatmeal') && TEXT == 'orange' && TEXT == 'oatmeal'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0orange\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0oatmeal\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void test_phraseFunction_eventQuery_excludeOnly() throws ParseException {
        String query = "((_Delayed_ = true) && (content:phrase(TEXT, 3, termOffsetMap, 'purple', 'pumpkin') && TEXT == 'purple' && TEXT == 'pumpkin'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0purple\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0pumpkin\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    @Test
    public void test_phraseFunction_tldQuery_excludeOnly() throws ParseException {
        String query = "((_Delayed_ = true) && (content:phrase(TEXT, 3, termOffsetMap, 'purple', 'pumpkin') && TEXT == 'purple' && TEXT == 'pumpkin'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0purple\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0pumpkin\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    // adjacency functions
    
    @Test
    public void test_adjacencyFunction_eventQuery_includeOnly() throws ParseException {
        String query = "content:adjacent(TEXT, termOffsetMap, 'red', 'car') && TEXT == 'red' && TEXT == 'car'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0red\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0car\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    @Test
    public void test_adjacencyFunction_tldQuery_includeOnly() throws ParseException {
        String query = "content:adjacent(TEXT, termOffsetMap, 'red', 'car') && TEXT == 'red' && TEXT == 'car'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.1\0red\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0car\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void test_adjacencyFunction_eventQuery_includeExclude() throws ParseException {
        String query = "(content:adjacent(TEXT, termOffsetMap, 'red', 'car') && TEXT == 'red' && TEXT == 'car') && "
                        + "((_Delayed_ = true) && (content:adjacent(TEXT, termOffsetMap, 'orange', 'oatmeal') && TEXT == 'orange' && TEXT == 'oatmeal'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0red\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0car\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0orange\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0oatmeal\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    @Test
    public void test_adjacencyFunction_tldQuery_includeExclude() throws ParseException {
        String query = "(content:adjacent(TEXT, termOffsetMap, 'red', 'car') && TEXT == 'red' && TEXT == 'car') && "
                        + "((_Delayed_ = true) && (content:adjacent(TEXT, termOffsetMap, 'orange', 'oatmeal') && TEXT == 'orange' && TEXT == 'oatmeal'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.1\0red\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0car\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0orange\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0oatmeal\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void test_adjacencyFunction_tldQuery_includeExclude_differentDocs() throws ParseException {
        String query = "(content:adjacent(TEXT, termOffsetMap, 'red', 'car') && TEXT == 'red' && TEXT == 'car') && "
                        + "((_Delayed_ = true) && (content:adjacent(TEXT, termOffsetMap, 'green', 'grass') && TEXT == 'green' && TEXT == 'grass'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.1\0red\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0car\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0green\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0grass\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void test_adjacencyFunction_eventQuery_excludeOnly() throws ParseException {
        String query = "((_Delayed_ = true) && (content:adjacent(TEXT, termOffsetMap, 'orange', 'oatmeal') && TEXT == 'orange' && TEXT == 'oatmeal'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0orange\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0oatmeal\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    @Test
    public void test_adjacencyFunction_tldQuery_excludeOnly() throws ParseException {
        String query = "((_Delayed_ = true) && (content:phrase(TEXT, termOffsetMap, 'orange', 'oatmeal') && TEXT == 'orange' && TEXT == 'oatmeal'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.1\0orange\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0oatmeal\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    // within functions
    
    @Test
    public void test_withinFunction_eventQuery_includeOnly() throws ParseException {
        String query = "content:within(TEXT, 2, termOffsetMap, 'yellow', 'bus') && TEXT == 'yellow' && TEXT == 'bus'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0yellow\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0bus\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    @Test
    public void test_withinFunction_tldQuery_includeOnly() throws ParseException {
        String query = "content:within(TEXT, 2, termOffsetMap, 'yellow', 'bus') && TEXT == 'yellow' && TEXT == 'bus'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.2\0yellow\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0bus\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void test_withinFunction_eventQuery_includeExclude() throws ParseException {
        String query = "(content:within(TEXT, 3, termOffsetMap, 'yellow', 'bus') && TEXT == 'yellow' && TEXT == 'bus') && "
                        + "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'green', 'grass') && TEXT == 'green' && TEXT == 'grass'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0yellow\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0bus\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0green\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0grass\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    @Test
    public void test_withinFunction_tldQuery_includeExclude() throws ParseException {
        String query = "(content:within(TEXT, 3, termOffsetMap, 'yellow', 'bus') && TEXT == 'yellow' && TEXT == 'bus') && "
                        + "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'green', 'grass') && TEXT == 'green' && TEXT == 'grass'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.2\0yellow\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0bus\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0green\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0grass\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void test_withinFunction_tldQuery_includeExclude_differentDocs() throws ParseException {
        String query = "(content:within(TEXT, 3, termOffsetMap, 'yellow', 'bus') && TEXT == 'yellow' && TEXT == 'bus') && "
                        + "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'purple', 'pumpkin') && TEXT == 'purple' && TEXT == 'pumpkin'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.2\0yellow\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0bus\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0purple\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0pumpkin\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void test_withinFunction_eventQuery_excludeOnly() throws ParseException {
        String query = "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'green', 'grass') && TEXT == 'green' && TEXT == 'grass'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0green\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0grass\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    @Test
    public void test_withinFunction_tldQuery_excludeOnly() throws ParseException {
        String query = "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'green', 'grass') && TEXT == 'green' && TEXT == 'grass'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.2\0green\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0grass\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    // no hit case
    
    @Test
    public void test_phraseFunction_eventQuery_include_noHit() throws ParseException {
        String query = "content:phrase(TEXT, termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    // special cases
    
    @Test
    public void testSpecialCase_tldQuery_includeExclude_includeFirst() throws ParseException {
        String query = "(content:within(TEXT, 3, termOffsetMap, 'red', 'car') && TEXT == 'red' && TEXT == 'car') && "
                        + "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'green', 'grass') && TEXT == 'green' && TEXT == 'grass'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.1\0red\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0car\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0green\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0grass\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void testSpecialCase_tldQuery_includeExclude_includeSecond() throws ParseException {
        String query = "(content:within(TEXT, 3, termOffsetMap, 'yellow', 'bus') && TEXT == 'yellow' && TEXT == 'bus') && "
                        + "((_Delayed_ = true) && (content:within(TEXT, 3, termOffsetMap, 'orange', 'oatmeal') && TEXT == 'orange' && TEXT == 'oatmeal'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.1\0orange\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.1\0oatmeal\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0yellow\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0.2\0bus\0TEXT"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true);
    }
    
    @Test
    public void testSpecialCase_eventQuery_include_repeatedTerm() throws ParseException {
        // when expanding functions, the values are treated as a set
        String query = "content:phrase(TEXT, termOffsetMap, 'red', 'red', 'car') && TEXT == 'red' && TEXT == 'car'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0red\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0car\0TEXT"));
        
        test(script, buildDocument(), expectedHits, buildSource());
    }
    
    // some tests where the field does not appear in the content function itself
    
    @Test
    public void test_phraseFunction_eventQuery_noFieldsInFunction_include() throws ParseException {
        String query = "content:phrase(termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        
        Multimap<String,String> tfFVs = HashMultimap.create();
        tfFVs.putAll("TEXT", Arrays.asList("blue", "house"));
        
        test(script, buildDocument(), expectedHits, buildSource(), tfFVs);
    }
    
    @Test
    public void test_phraseFunction_tldQuery_noFieldsInFunction_include() throws ParseException {
        String query = "content:phrase(termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        
        Multimap<String,String> tfFVs = HashMultimap.create();
        tfFVs.putAll("TEXT", Arrays.asList("blue", "house"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true, tfFVs);
    }
    
    @Test
    public void test_phraseFunction_eventQuery_noFieldsInFunction_includeExclude() throws ParseException {
        String query = "(content:phrase(termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house') && "
                        + "((_Delayed_ = true) && (content:phrase(termOffsetMap, 'purple', 'pumpkin') && TEXT == 'purple' && TEXT == 'pumpkin'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0purple\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0pumpkin\0TEXT"));
        
        Multimap<String,String> tfFVs = HashMultimap.create();
        tfFVs.putAll("TEXT", Arrays.asList("blue", "house", "purple", "pumpkin"));
        
        test(script, buildDocument(), expectedHits, buildSource(), tfFVs);
    }
    
    @Test
    public void test_phraseFunction_tldQuery_noFieldsInFunction_includeExclude() throws ParseException {
        String query = "(content:phrase(termOffsetMap, 'blue', 'house') && TEXT == 'blue' && TEXT == 'house') && "
                        + "((_Delayed_ = true) && (content:phrase(termOffsetMap, 'purple', 'pumpkin') && TEXT == 'purple' && TEXT == 'pumpkin'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blue\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0house\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0purple\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0pumpkin\0TEXT"));
        
        Multimap<String,String> tfFVs = HashMultimap.create();
        tfFVs.putAll("TEXT", Arrays.asList("blue", "house", "purple", "pumpkin"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true, tfFVs);
    }
    
    @Test
    public void test_phraseFunction_eventQuery_noFieldsInFunction_exclude() throws ParseException {
        String query = "((_Delayed_ = true) && (content:phrase(termOffsetMap, 'purple', 'pumpkin') && TEXT == 'purple' && TEXT == 'pumpkin'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0purple\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0pumpkin\0TEXT"));
        
        Multimap<String,String> tfFVs = HashMultimap.create();
        tfFVs.putAll("TEXT", Arrays.asList("purple", "pumpkin"));
        
        test(script, buildDocument(), expectedHits, buildSource(), tfFVs);
    }
    
    @Test
    public void test_phraseFunction_tldQuery_noFieldsInFunction_exclude() throws ParseException {
        String query = "((_Delayed_ = true) && (content:phrase(termOffsetMap, 'purple', 'pumpkin') && TEXT == 'purple' && TEXT == 'pumpkin'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0purple\0TEXT"));
        expectedHits.add(new Text("datatype\0uid0\0pumpkin\0TEXT"));
        
        Multimap<String,String> tfFVs = HashMultimap.create();
        tfFVs.putAll("TEXT", Arrays.asList("purple", "pumpkin"));
        
        test(script, buildTldDocument(), expectedHits, buildTldSource(), true, tfFVs);
    }
    
    // multi-fielded queries with no fields in function
    
    @Test
    public void test_phraseFunction_eventQuery_noFieldsInFunction_multiFielded_include() throws ParseException {
        String query = "content:phrase(termOffsetMap, 'brown', 'fox') && ((TEXT_A == 'brown' && TEXT_A == 'fox') || (TEXT_B == 'brown' && TEXT_B == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT_B"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT_B"));
        
        Multimap<String,String> tfFVs = HashMultimap.create();
        tfFVs.putAll("TEXT_A", Arrays.asList("brown", "fox"));
        tfFVs.putAll("TEXT_B", Arrays.asList("brown", "fox"));
        
        test(script, buildMultiFieldedDocument(), expectedHits, buildMultiFieldedSource(), tfFVs);
    }
    
    @Test
    public void test_phraseFunction_tldQuery_noFieldsInFunction_multiFielded_include() throws ParseException {
        String query = "content:phrase(termOffsetMap, 'brown', 'fox') && ((TEXT_A == 'brown' && TEXT_A == 'fox') || (TEXT_B == 'brown' && TEXT_B == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.2\0brown\0TEXT_B"));
        expectedHits.add(new Text("datatype\0uid0.2\0fox\0TEXT_B"));
        
        Multimap<String,String> tfFVs = HashMultimap.create();
        tfFVs.putAll("TEXT_A", Arrays.asList("brown", "fox"));
        tfFVs.putAll("TEXT_B", Arrays.asList("brown", "fox"));
        
        test(script, buildMultiFieldedTldDocument(), expectedHits, buildMultiFieldedTldSource(), true, tfFVs);
    }
    
    // multi fielded function, only one field hits
    @Test
    public void test_phraseFunction_eventQuery_multiFielded_include() throws ParseException {
        String query = "content:phrase((TEXT_A | TEXT_B), termOffsetMap, 'brown', 'fox') && "
                        + "((TEXT_A == 'brown' && TEXT_A == 'fox') || (TEXT_B == 'brown' && TEXT_B == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT_B"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT_B"));
        
        test(script, buildMultiFieldedDocument(), expectedHits, buildMultiFieldedSource());
    }
    
    // multi fielded function, every field hits
    @Test
    public void test_phraseFunction_eventQuery_multiFielded_include_twoFunctions() throws ParseException {
        String query = "content:phrase((TEXT_A | TEXT_B), termOffsetMap, 'the', 'quick') && "
                        + "((TEXT_A == 'the' && TEXT_A == 'quick') || (TEXT_B == 'the' && TEXT_B == 'quick')) || "
                        + "content:phrase((TEXT_A | TEXT_B), termOffsetMap, 'brown', 'fox') && "
                        + "((TEXT_A == 'brown' && TEXT_A == 'fox') || (TEXT_B == 'brown' && TEXT_B == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0the\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0quick\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT_B"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT_B"));
        
        test(script, buildMultiFieldedDocument(), expectedHits, buildMultiFieldedSource());
    }
    
    // multi fielded function, one phrase is a partial hit -- should be fully excluded from search space.
    @Test
    public void test_phraseFunction_eventQuery_multiFielded_includes_halfHit() throws ParseException {
        String query = "content:phrase((TEXT_A | TEXT_B), termOffsetMap, 'red', 'fox') && "
                        + "((TEXT_A == 'red' && TEXT_A == 'fox') || (TEXT_B == 'red' && TEXT_B == 'fox')) || "
                        + "content:phrase((TEXT_A | TEXT_B), termOffsetMap, 'brown', 'fox') && "
                        + "((TEXT_A == 'brown' && TEXT_A == 'fox') || (TEXT_B == 'brown' && TEXT_B == 'fox'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0brown\0TEXT_B"));
        expectedHits.add(new Text("datatype\0uid0\0fox\0TEXT_B"));
        
        test(script, buildMultiFieldedDocument(), expectedHits, buildMultiFieldedSource());
    }
    
    // multi fielded negated function, one phrase is a partial hit -- should be fully excluded from search space.
    @Test
    public void test_phraseFunction_eventQuery_multiFielded_excludes_halfHit() throws ParseException {
        String query = "((_Delayed_ = true) && (content:phrase((TEXT_A | TEXT_B), termOffsetMap, 'blip', 'blap') && "
                        + "((TEXT_A == 'blip' && TEXT_A == 'blap') || (TEXT_B == 'blip' && TEXT_B == 'blap')))) || "
                        + "((_Delayed_ = true) && (content:phrase((TEXT_A | TEXT_B), termOffsetMap, 'bleep', 'blarp') && "
                        + "((TEXT_A == 'bleep' && TEXT_A == 'blarp') || (TEXT_B == 'bleep' && TEXT_B == 'blarp'))))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        // For the negated event query case, we bypass the FI lookup an simply populate a TF search space from all content function field value pairs. The cost
        // to scan the TF range is *probably* offset by not reaching back to the FI.
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0\0blip\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0blip\0TEXT_B"));
        expectedHits.add(new Text("datatype\0uid0\0blap\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0blap\0TEXT_B"));
        expectedHits.add(new Text("datatype\0uid0\0bleep\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0bleep\0TEXT_B"));
        expectedHits.add(new Text("datatype\0uid0\0blarp\0TEXT_A"));
        expectedHits.add(new Text("datatype\0uid0\0blarp\0TEXT_B"));
        
        test(script, buildMultiFieldedDocument(), expectedHits, buildMultiFieldedSource());
    }
    
    // multi fielded negated function, one phrase is a partial hit -- should be fully excluded from search space.
    @Test
    public void test_phraseFunction_tldQuery_multiFielded_excludes_halfHit() throws ParseException {
        String query = "((_Delayed_ = true) && (content:phrase((TEXT_A | TEXT_B), termOffsetMap, 'blip', 'blap') && "
                        + "((TEXT_A == 'blip' && TEXT_A == 'blap') || (TEXT_B == 'blip' && TEXT_B == 'blap')))) || "
                        + "((_Delayed_ = true) && (content:phrase((TEXT_A | TEXT_B), termOffsetMap, 'bleep', 'blarp') && "
                        + "((TEXT_A == 'bleep' && TEXT_A == 'blarp') || (TEXT_B == 'bleep' && TEXT_B == 'blarp'))))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        
        // first phrase, term 'blap' does not appear thus first phrase is excluded.
        // second phrase has hits on both 'bleep' and 'blarp', thus hits are built from the phrase.
        TreeSet<Text> expectedHits = new TreeSet<>();
        expectedHits.add(new Text("datatype\0uid0.2\0bleep\0TEXT_B"));
        expectedHits.add(new Text("datatype\0uid0.2\0blarp\0TEXT_B"));
        
        test(script, buildMultiFieldedTldDocument(), expectedHits, buildMultiFieldedTldSource(), true);
    }
    
    private void test(ASTJexlScript script, Document doc, TreeSet<Text> expected, SortedKeyValueIterator<Key,Value> source) {
        test(script, doc, expected, source, false);
    }
    
    private void test(ASTJexlScript script, Document doc, TreeSet<Text> expected, SortedKeyValueIterator<Key,Value> source,
                    Multimap<String,String> tfFieldValues) {
        test(script, doc, expected, source, false, tfFieldValues);
    }
    
    private void test(ASTJexlScript script, Document doc, TreeSet<Text> expected, SortedKeyValueIterator<Key,Value> source, boolean isTld) {
        test(script, doc, expected, source, isTld, HashMultimap.create());
    }
    
    private void test(ASTJexlScript script, Document doc, TreeSet<Text> expected, SortedKeyValueIterator<Key,Value> source, boolean isTld,
                    Multimap<String,String> tfFieldValues) {
        TermFrequencyConfig tfConfig = new TermFrequencyConfig();
        tfConfig.setSource(source);
        tfConfig.setScript(script);
        tfConfig.setTld(isTld);
        tfConfig.setIterEnv(new TestIteratorEnvironment());
        
        TermFrequencyHitFunction hitFunction = new TermFrequencyHitFunction(tfConfig, tfFieldValues);
        
        Key docKey = new Key("shard", "datatype\0uid0");
        TreeSet<Text> hits = hitFunction.apply(docKey, doc);
        
        assertEquals(expected, hits);
    }
    
    public static class TestIteratorEnvironment implements IteratorEnvironment {
        @Override
        public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String s) throws IOException {
            return null;
        }
        
        @Override
        public AccumuloConfiguration getConfig() {
            return null;
        }
        
        @Override
        public IteratorUtil.IteratorScope getIteratorScope() {
            return null;
        }
        
        @Override
        public boolean isFullMajorCompaction() {
            return false;
        }
        
        @Override
        public void registerSideChannel(SortedKeyValueIterator<Key,Value> sortedKeyValueIterator) {
            
        }
        
        @Override
        public Authorizations getAuthorizations() {
            return null;
        }
        
        @Override
        public IteratorEnvironment cloneWithSamplingEnabled() {
            return null;
        }
        
        @Override
        public boolean isSamplingEnabled() {
            return false;
        }
        
        @Override
        public SamplerConfiguration getSamplerConfiguration() {
            return null;
        }
    }
    
    // blue, red, yellow are positive terms
    // purple, orange, green are negative terms (i.e., will only appear in the FI source).
    private Document buildDocument() {
        Document doc = new Document();
        doc.put("TEXT", new PreNormalizedAttribute("blue", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("house", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("red", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("car", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("yellow", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("bus", new Key("shard", "datatype\0uid0"), true));
        // additional values that should not show up in the search space
        doc.put("TEXT", new PreNormalizedAttribute("see", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("spot", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("run", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("look", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("jane", new Key("shard", "datatype\0uid0"), true));
        return doc;
    }
    
    private Document buildTldDocument() {
        Document doc = new Document();
        doc.put("TEXT", new PreNormalizedAttribute("blue", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("house", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("red", new Key("shard", "datatype\0uid0.1"), true));
        doc.put("TEXT", new PreNormalizedAttribute("car", new Key("shard", "datatype\0uid0.1"), true));
        doc.put("TEXT", new PreNormalizedAttribute("yellow", new Key("shard", "datatype\0uid0.2"), true));
        doc.put("TEXT", new PreNormalizedAttribute("bus", new Key("shard", "datatype\0uid0.2"), true));
        // additional values that should not show up in the search space
        doc.put("TEXT", new PreNormalizedAttribute("see", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT", new PreNormalizedAttribute("spot", new Key("shard", "datatype\0uid0.1"), true));
        doc.put("TEXT", new PreNormalizedAttribute("run", new Key("shard", "datatype\0uid0.2"), true));
        doc.put("TEXT", new PreNormalizedAttribute("look", new Key("shard", "datatype\0uid0.1"), true));
        doc.put("TEXT", new PreNormalizedAttribute("jane", new Key("shard", "datatype\0uid0"), true));
        return doc;
    }
    
    // 'the quick brown fox'
    private Document buildMultiFieldedDocument() {
        Document doc = new Document();
        doc.put("TEXT_A", new PreNormalizedAttribute("the", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT_A", new PreNormalizedAttribute("quick", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT_B", new PreNormalizedAttribute("brown", new Key("shard", "datatype\0uid0"), true));
        doc.put("TEXT_B", new PreNormalizedAttribute("fox", new Key("shard", "datatype\0uid0"), true));
        return doc;
    }
    
    // 'the quick brown fox'
    private Document buildMultiFieldedTldDocument() {
        Document doc = new Document();
        doc.put("TEXT_A", new PreNormalizedAttribute("the", new Key("shard", "datatype\0uid0.1"), true));
        doc.put("TEXT_A", new PreNormalizedAttribute("quick", new Key("shard", "datatype\0uid0.1"), true));
        doc.put("TEXT_B", new PreNormalizedAttribute("brown", new Key("shard", "datatype\0uid0.2"), true));
        doc.put("TEXT_B", new PreNormalizedAttribute("fox", new Key("shard", "datatype\0uid0.2"), true));
        return doc;
    }
    
    private SortedKeyValueIterator<Key,Value> buildSource() {
        // List is unsorted, SortedListKeyValueIterator will sort entries by key during init
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "blue\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "house\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "red\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "car\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "yellow\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "bus\0datatype\0uid0"), new Value()));
        // terms from negated phrases are not built into the document.
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "purple\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "pumpkin\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "orange\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "oatmeal\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "green\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "grass\0datatype\0uid0"), new Value()));
        // additional values that should not show up in the search space
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "see\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "spot\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "run\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "look\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "jane\0datatype\0uid0"), new Value()));
        return new SortedListKeyValueIterator(data);
    }
    
    private SortedKeyValueIterator<Key,Value> buildTldSource() {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "blue\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "house\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "red\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "car\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "yellow\0datatype\0uid0.2"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "bus\0datatype\0uid0.2"), new Value()));
        // And negated terms
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "purple\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "pumpkin\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "orange\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "oatmeal\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "green\0datatype\0uid0.2"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "grass\0datatype\0uid0.2"), new Value()));
        // additional values that should not show up in the search space
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "see\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "spot\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "run\0datatype\0uid0.2"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "look\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT", "jane\0datatype\0uid0"), new Value()));
        return new SortedListKeyValueIterator(data);
    }
    
    private SortedKeyValueIterator<Key,Value> buildMultiFieldedSource() {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_A", "the\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_A", "quick\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_B", "brown\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_B", "fox\0datatype\0uid0"), new Value()));
        // add some values for negated functions, i.e., values that do not appear in the document via positive fi iterators.
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_A", "blip\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_A", "bloop\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_B", "bleep\0datatype\0uid0"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_B", "blarp\0datatype\0uid0"), new Value()));
        return new SortedListKeyValueIterator(data);
    }
    
    private SortedKeyValueIterator<Key,Value> buildMultiFieldedTldSource() {
        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_A", "the\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_A", "quick\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_B", "brown\0datatype\0uid0.2"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_B", "fox\0datatype\0uid0.2"), new Value()));
        // add some values for negated functions, i.e., values that do not appear in the document via positive fi iterators.
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_A", "blip\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_A", "bloop\0datatype\0uid0.1"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_B", "bleep\0datatype\0uid0.2"), new Value()));
        data.add(new SimpleEntry<>(new Key("shard", "fi\0TEXT_B", "blarp\0datatype\0uid0.2"), new Value()));
        return new SortedListKeyValueIterator(data);
    }
}
