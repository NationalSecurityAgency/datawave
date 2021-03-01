package datawave.query.function;

import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.util.Tuple3;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

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
}
