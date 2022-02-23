package datawave.query.function;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.jexl.functions.TermFrequencyList;
import datawave.query.util.Tuple3;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JexlEvaluationTest {
    
    @Test
    public void testSimpleQuery() {
        String query = "FOO == 'bar'";
        Document d = new Document();
        d.put("FOO", new Content("bar", new Key("shard", "datatype\0uid"), true));
        
        JexlEvaluation evaluation = new JexlEvaluation(query);
        DatawaveJexlContext context = new DatawaveJexlContext();
        d.visit(Collections.singleton("FOO"), context);
        
        boolean result = evaluation.apply(new Tuple3<>(new Key("shard", "datatype\0uid"), d, context));
        assertTrue(result);
    }
    
    @Test
    public void testRegexIntersection() {
        String query = "FOO == 'bar' && FOO =~ 'baz.*'";
        Document d = new Document();
        d.put("FOO", new Content("bar", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));
        
        JexlEvaluation evaluation = new JexlEvaluation(query);
        DatawaveJexlContext context = new DatawaveJexlContext();
        d.visit(Collections.singleton("FOO"), context);
        
        boolean result = evaluation.apply(new Tuple3<>(new Key("shard", "datatype\0uid"), d, context));
        assertTrue(result);
        
    }
    
    @Test
    public void testRegexUnion() {
        String query = "FOO == 'bar' || FOO =~ 'baz.*'";
        Document d = new Document();
        d.put("FOO", new Content("bar", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));
        
        JexlEvaluation evaluation = new JexlEvaluation(query);
        DatawaveJexlContext context = new DatawaveJexlContext();
        d.visit(Collections.singleton("FOO"), context);
        
        boolean result = evaluation.apply(new Tuple3<>(new Key("shard", "datatype\0uid"), d, context));
        assertTrue(result);
    }
    
    @Test
    public void testSomeFilterFunctions() {
        String query = "ANCHOR == 'a' && filter:includeRegex(FOO, 'baz.*')";
        Document d = new Document();
        d.put("ANCHOR", new Content("a", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));
        
        // Case 1: Single fielded filter function, field is present
        evaluate(query, d);
        
        query = "ANCHOR == 'a' && filter:includeRegex((FOO||FOO2||FOO3), 'baz.*')";
        String orderMattersQuery = "ANCHOR == 'a' && filter:includeRegex((FOO3||FOO2||FOO), 'baz.*')";
        d = new Document();
        d.put("ANCHOR", new Content("a", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));
        d.put("FOO3", new Content("bazaar", new Key("shard", "datatype\0uid"), true));
        
        // Case 2: Multi-fielded filter function, both fields present
        evaluate(query, d);
        evaluate(orderMattersQuery, d);
        
        d = new Document();
        d.put("ANCHOR", new Content("a", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("bazaar", new Key("shard", "datatype\0uid"), true));
        d.put("FOO3", new Content("nohit", new Key("shard", "datatype\0uid"), true));
        
        // Case 3: Multi-fielded filter function, only first field is present
        evaluate(query, d);
        evaluate(orderMattersQuery, d);
        
        d = new Document();
        d.put("ANCHOR", new Content("a", new Key("shard", "datatype\0uid"), true));
        d.put("FOO", new Content("nohit", new Key("shard", "datatype\0uid"), true));
        d.put("FOO3", new Content("bazaar", new Key("shard", "datatype\0uid"), true));
        
        // Case 4: Multi-fielded filter function, only second field is present
        evaluate(query, d);
        evaluate(orderMattersQuery, d);
    }
    
    // Assume fields are {ANCHOR, FOO, FOO2} and a constant doc key
    private void evaluate(String query, Document d) {
        JexlEvaluation evaluation = new JexlEvaluation(query);
        
        DatawaveJexlContext context = new DatawaveJexlContext();
        d.visit(Arrays.asList("ANCHOR", "FOO", "FOO2", "FOO3"), context);
        
        boolean result = evaluation.apply(new Tuple3<>(new Key("shard", "datatype\0uid"), d, context));
        assertTrue(result);
    }
    
    @Test
    public void testContentPhraseFunction() {
        String query = "FOO == 'bar' && TOKFIELD == 'big' && TOKFIELD == 'red' && TOKFIELD == 'dog' && content:phrase(termOffsetMap, 'big', 'red', 'dog')";
        
        Map<String,TermFrequencyList> map = new HashMap<>();
        map.put("big", buildTfList("TOKFIELD", 1));
        map.put("red", buildTfList("TOKFIELD", 2));
        map.put("dog", buildTfList("TOKFIELD", 3));
        
        DatawaveJexlContext context = new DatawaveJexlContext();
        context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, map);
        
        Key docKey = new Key("shard", "datatype\0uid");
        
        Document d = new Document();
        d.put("FOO", new Content("bar", docKey, true));
        d.put("TOKFIELD", new Content("big", docKey, true));
        d.put("TOKFIELD", new Content("red", docKey, true));
        d.put("TOKFIELD", new Content("dog", docKey, true));
        d.visit(Arrays.asList("FOO", "TOKFIELD"), context);
        
        JexlEvaluation evaluation = new JexlEvaluation(query, new HitListArithmetic());
        
        Tuple3<Key,Document,DatawaveJexlContext> tuple = new Tuple3<>(docKey, d, context);
        boolean result = evaluation.apply(tuple);
        assertTrue(result);
        
        // assert that "big red dog" came back in the hit terms
        boolean foundPhrase = false;
        Attributes attrs = (Attributes) d.get("HIT_TERM");
        for (Attribute<?> attr : attrs.getAttributes()) {
            if (attr.getData().equals("TOKFIELD:big red dog")) {
                foundPhrase = true;
            }
        }
        assertEquals(5, attrs.size());
        assertTrue(foundPhrase);
    }
    
    @Test
    public void testCompareFunction() {
        // eq op
        String query = "FOO == 'bar' && filter:compare(FIELD_A,'==','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_A,'==','any',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'==','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'==','any',FIELD_B)";
        testCompare(query, true);
        
        // eq op, alternate form
        query = "FOO == 'bar' && filter:compare(FIELD_A,'=','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_A,'=','any',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'=','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'=','any',FIELD_B)";
        testCompare(query, true);
        
        // lt op
        query = "FOO == 'bar' && filter:compare(FIELD_A,'<','all',FIELD_B)";
        testCompare(query, true);
        
        query = "FOO == 'bar' && filter:compare(FIELD_A,'<','any',FIELD_B)";
        testCompare(query, true);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'<','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'<','any',FIELD_B)";
        testCompare(query, true);
        
        // le op
        query = "FOO == 'bar' && filter:compare(FIELD_A,'<=','all',FIELD_B)";
        testCompare(query, true);
        
        query = "FOO == 'bar' && filter:compare(FIELD_A,'<=','any',FIELD_B)";
        testCompare(query, true);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'<=','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'<=','any',FIELD_B)";
        testCompare(query, true);
        
        // gt op
        query = "FOO == 'bar' && filter:compare(FIELD_A,'>','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_A,'>','any',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'>','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'>','any',FIELD_B)";
        testCompare(query, true);
        
        // ge op
        query = "FOO == 'bar' && filter:compare(FIELD_A,'>=','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_A,'>=','any',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'>=','all',FIELD_B)";
        testCompare(query, false);
        
        query = "FOO == 'bar' && filter:compare(FIELD_C,'>=','any',FIELD_B)";
        testCompare(query, true);
    }
    
    private void testCompare(String query, boolean expected) {
        
        // populate doc
        Key docKey = new Key("shard", "datatype\0uid");
        Document d = new Document();
        d.put("FOO", new Content("bar", docKey, true));
        d.put("FIELD_A", new Content("apple", docKey, true));
        d.put("FIELD_A", new Content("banana", docKey, true));
        d.put("FIELD_B", new Content("xylophone", docKey, true));
        d.put("FIELD_B", new Content("zephyr", docKey, true));
        d.put("FIELD_C", new Content("zebra", docKey, true));
        d.put("FIELD_C", new Content("zephyr", docKey, true));
        
        // populate context from doc
        DatawaveJexlContext context = new DatawaveJexlContext();
        d.visit(Arrays.asList("FOO", "FIELD_A", "FIELD_B", "FIELD_C"), context);
        
        Tuple3<Key,Document,DatawaveJexlContext> tuple = new Tuple3<>(docKey, d, context);
        
        JexlEvaluation evaluation = new JexlEvaluation(query, new HitListArithmetic());
        boolean result = evaluation.apply(tuple);
        
        assertEquals(expected, result);
    }
    
    private TermFrequencyList buildTfList(String field, int... offsets) {
        TermFrequencyList.Zone zone = buildZone(field);
        List<TermWeightPosition> position = buildTermWeightPositions(offsets);
        return new TermFrequencyList(Maps.immutableEntry(zone, position));
    }
    
    private TermFrequencyList.Zone buildZone(String field) {
        return new TermFrequencyList.Zone(field, true, "shard\0datatype\0uid");
    }
    
    private List<TermWeightPosition> buildTermWeightPositions(int... offsets) {
        List<TermWeightPosition> list = new ArrayList<>();
        for (int offset : offsets) {
            list.add(new TermWeightPosition.Builder().setOffset(offset).setZeroOffsetMatch(true).build());
        }
        return list;
    }
}
