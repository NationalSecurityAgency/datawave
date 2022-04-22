package datawave.query.postprocessing.tf;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.TermWeight;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TermOffsetPopulatorTest {
    
    @Test
    public void testGetContentFunctions() throws ParseException {
        String query = "content:phrase(TEXT, termOffsetMap, 'quick', 'brown', 'fox')";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        Multimap<String,Function> functions = TermOffsetPopulator.getContentFunctions(script);
        
        assertEquals(1, functions.keySet().size());
        assertTrue(functions.containsKey("phrase"));
        
        query = "content:within(TEXT, 3,'quick', 'fox')";
        script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        functions = TermOffsetPopulator.getContentFunctions(script);
        
        assertEquals(1, functions.keySet().size());
        assertTrue(functions.containsKey("within"));
        
        query = "content:adjacent(TEXT, termOffsetMap, 'quick', 'brown', 'fox')";
        script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        functions = TermOffsetPopulator.getContentFunctions(script);
        
        assertEquals(1, functions.keySet().size());
        assertTrue(functions.containsKey("adjacent"));
        
        // And multi-function query
        query = "content:adjacent(TEXT, termOffsetMap, 'quick', 'brown', 'fox') || content:within(TEXT, 3,'quick', 'fox')";
        script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        functions = TermOffsetPopulator.getContentFunctions(script);
        
        assertEquals(2, functions.keySet().size());
        assertTrue(functions.containsKey("adjacent"));
        assertTrue(functions.containsKey("within"));
    }
    
    // for query like 'content:within(TEXT, 3,'quick', 'fox')'
    @Test
    public void testGetFieldValues() throws ParseException {
        String query = "content:within(TEXT, 3,'quick', 'fox')";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Multimap<String,String> tfFields = HashMultimap.create();
        tfFields.putAll("TEXT", Arrays.asList("quick", "fox"));
        
        SortedMapIterator iter = createDataIter();
        
        TermOffsetPopulator populator = new TermOffsetPopulator(tfFields, null, null, iter);
        
        Multimap<String,String> fvs = populator.getTermFrequencyFieldValues();
        assertEquals(2, fvs.size());
        assertTrue(fvs.containsEntry("TEXT", "quick"));
        assertTrue(fvs.containsEntry("TEXT", "fox"));
    }
    
    @Test
    public void testGetFieldValuesFromScript() throws ParseException {
        String query = "content:within(TEXT, 3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Set<String> contentExpansionFields = Sets.newHashSet("TEXT");
        
        Set<String> tfFields = Sets.newHashSet("TEXT");
        
        Multimap<String,Class<? extends Type<?>>> datatypes = HashMultimap.create();
        datatypes.put("TEXT", NoOpType.class);
        
        Multimap<String,String> fvs = TermOffsetPopulator.getTermFrequencyFieldValues(script, contentExpansionFields, tfFields, datatypes);
        assertEquals(2, fvs.size());
        assertTrue(fvs.containsEntry("TEXT", "quick"));
        assertTrue(fvs.containsEntry("TEXT", "fox"));
    }
    
    @Test
    public void testGetFieldValuesFromScript_valuesNotInFunction() throws ParseException {
        String query = "content:within(3, termOffsetMap, 'quick', 'fox') && TEXT == 'quick' && TEXT == 'fox'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        
        Set<String> contentExpansionFields = Sets.newHashSet("TEXT");
        
        Set<String> tfFields = Sets.newHashSet("TEXT");
        
        Multimap<String,Class<? extends Type<?>>> datatypes = HashMultimap.create();
        datatypes.put("TEXT", NoOpType.class);
        
        Multimap<String,String> fvs = TermOffsetPopulator.getTermFrequencyFieldValues(script, contentExpansionFields, tfFields, datatypes);
        assertEquals(2, fvs.size());
        assertTrue(fvs.containsEntry("TEXT", "quick"));
        assertTrue(fvs.containsEntry("TEXT", "fox"));
    }
    
    // for query like 'content:within(TEXT, 3,'quick', 'fox')'
    @Test
    public void testGetContextMap() {
        Multimap<String,String> tfFields = HashMultimap.create();
        tfFields.putAll("TEXT", Arrays.asList("quick", "fox"));
        
        SortedMapIterator iter = createDataIter();
        
        TermOffsetPopulator populator = new TermOffsetPopulator(tfFields, Sets.newHashSet("TEXT"), null, iter);
        
        Key docKey = new Key("shard", "datatype\0uid0");
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid0\0fox\0TEXT"));
        searchSpace.add(new Text("datatype\0uid0\0quick\0TEXT"));
        Map<String,Object> contextMap = populator.getContextMap(docKey, searchSpace);
        
        // ensure context map was populated
        assertNotNull(contextMap);
        assertEquals(1, contextMap.size());
        assertTrue(contextMap.containsKey(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME));
        
        // ensure termOffsetMap has correct values
        TermOffsetMap termOffsetMap = (TermOffsetMap) contextMap.get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME);
        assertNotNull(termOffsetMap.getTermFrequencyList("fox"));
        assertNotNull(termOffsetMap.getTermFrequencyList("quick"));
    }
    
    // A term offset map like this will be defeated during ContentFunction.initializes()
    @Test
    public void testPotentialErrorCase() {
        Multimap<String,String> tfFields = HashMultimap.create();
        tfFields.putAll("TEXT", Arrays.asList("blue", "fish"));
        
        TreeMap<Key,Value> data = new TreeMap<>();
        data.put(createTFKey("uid0.1", "TEXT", "blue"), createTFValue(1, 8, 12, 22));
        data.put(createTFKey("uid0.2", "TEXT", "fish"), createTFValue(2, 54, 120));
        
        SortedMapIterator source = new SortedMapIterator(data);
        
        TermOffsetPopulator populator = new TermOffsetPopulator(tfFields, Sets.newHashSet("TEXT"), null, source);
        
        Key docKey = new Key("shard", "datatype\0uid0");
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid0.1\0blue\0TEXT"));
        searchSpace.add(new Text("datatype\0uid0.2\0fish\0TEXT"));
        Map<String,Object> contextMap = populator.getContextMap(docKey, searchSpace);
        
        // ensure context map was populated
        assertNotNull(contextMap);
        assertEquals(1, contextMap.size());
        assertTrue(contextMap.containsKey(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME));
        
        // ensure termOffsetMap has correct values
        TermOffsetMap termOffsetMap = (TermOffsetMap) contextMap.get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME);
        assertNotNull(termOffsetMap.getTermFrequencyList("blue"));
        assertNotNull(termOffsetMap.getTermFrequencyList("fish"));
    }
    
    @Test
    public void testTraversalOfMalformedData() {
        Multimap<String,String> tfFields = HashMultimap.create();
        tfFields.putAll("TEXT", Arrays.asList("quick", "fox"));
        
        SortedMapIterator iter = new SortedMapIterator(createMalformedData());
        
        TermOffsetPopulator populator = new TermOffsetPopulator(tfFields, Sets.newHashSet("TEXT"), null, iter);
        
        Key docKey = new Key("shard", "datatype\0uid0");
        TreeSet<Text> searchSpace = new TreeSet<>();
        searchSpace.add(new Text("datatype\0uid0\0fox\0TEXT"));
        searchSpace.add(new Text("datatype\0uid0\0quick\0TEXT"));
        Map<String,Object> contextMap = populator.getContextMap(docKey, searchSpace);
        
        // ensure context map was populated
        assertNotNull(contextMap);
        assertEquals(1, contextMap.size());
        assertTrue(contextMap.containsKey(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME));
        
        // ensure termOffsetMap has correct values
        TermOffsetMap termOffsetMap = (TermOffsetMap) contextMap.get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME);
        assertNull(termOffsetMap.getTermFrequencyList("fox")); // the key for 'fox' is malformed
        assertNotNull(termOffsetMap.getTermFrequencyList("quick")); // the key for 'quick' is correct
    }
    
    private SortedMapIterator createDataIter() {
        TreeMap<Key,Value> data = createData();
        return new SortedMapIterator(data);
    }
    
    private TreeMap<Key,Value> createData() {
        TreeMap<Key,Value> data = new TreeMap<>();
        
        // Ideally this would generate tokens from a phrase like
        // Phrases take the form "FIELD UID $phrase"
        // "TEXT uid0 a quick brown fox jumped over the lazy dog",
        // "TEXT uid1 a small orange dog leapt over a green cat",
        
        // '0 1 2 3 4 5 6 7 8'
        // 'the quick brown fox jumped over the lazy dog'
        data.put(createTFKey("uid0", "TEXT", "the"), createTFValue(0, 6));
        data.put(createTFKey("uid0", "TEXT", "quick"), createTFValue(1));
        data.put(createTFKey("uid0", "TEXT", "brown"), createTFValue(2));
        data.put(createTFKey("uid0", "TEXT", "fox"), createTFValue(3));
        data.put(createTFKey("uid0", "TEXT", "jumped"), createTFValue(4));
        data.put(createTFKey("uid0", "TEXT", "over"), createTFValue(5));
        data.put(createTFKey("uid0", "TEXT", "lazy"), createTFValue(7));
        data.put(createTFKey("uid0", "TEXT", "dog"), createTFValue(8));
        return data;
    }
    
    private Key createTFKey(String uid, String field, String value) {
        Text row = new Text("shard");
        Text cf = new Text("tf");
        Text cq = new Text("datatype\0" + uid + "\0" + value + "\0" + field);
        return new Key(row, cf, cq);
    }
    
    private Value createTFValue(int... offsets) {
        List<Integer> ints = Arrays.stream(offsets).boxed().collect(Collectors.toList());
        return createTFValue(ints);
    }
    
    private Value createTFValue(List<Integer> offsets) {
        if (offsets == null) {
            return new Value(TermWeight.Info.newBuilder().build().toByteArray());
        }
        return new Value(TermWeight.Info.newBuilder().addAllTermOffset(offsets).build().toByteArray());
    }
    
    private TreeMap<Key,Value> createMalformedData() {
        TreeMap<Key,Value> data = new TreeMap<>();
        data.put(new Key("shard", "tf", "datatype\0uid0\0the\0TEXT"), createTFValue(0, 6));
        data.put(new Key("shard", "tf", "datatype\0uid0\0quick\0TEXT"), createTFValue(1));
        data.put(new Key("shard", "tf", "datatype\0uid0\0brown"), createTFValue(2));
        data.put(new Key("shard", "tf", "datatype\0uid0\0fox\0\0"), createTFValue(3));
        data.put(new Key("shard", "tf", "datatype\0jumped"), createTFValue(4));
        data.put(new Key("shard", "tf", "datatype\0uid0\0over\0TEXT"), createTFValue(5));
        data.put(new Key("shard", "tf", "datatype\0uid0lazyTEXT"), createTFValue(7));
        data.put(new Key("shard", "tf", "datatypeuid0\0dog\0TEXT"), createTFValue(8));
        return data;
    }
}
