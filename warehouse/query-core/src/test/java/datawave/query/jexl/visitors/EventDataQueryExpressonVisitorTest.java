package datawave.query.jexl.visitors;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.ExpressionFilter;
import datawave.query.util.MockDateIndexHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;

public class EventDataQueryExpressonVisitorTest {
    
    private static final String DATATYPE = "ingest";
    private static final String UID = "uid";
    private static final ColumnVisibility cv1 = new ColumnVisibility("A&B&C&(D|E|F)");
    private AttributeFactory attrFactory;
    
    @Before
    public void setupTypeMetadata() {
        String lcNoDiacritics = LcNoDiacriticsType.class.getName();
        String number = NumberType.class.getName();
        
        TypeMetadata md = new TypeMetadata();
        md.put("FOO", DATATYPE, lcNoDiacritics);
        md.put("BAZ", DATATYPE, number);
        md.put("BAR", DATATYPE, lcNoDiacritics);
        md.put("BAR", DATATYPE, number);
        
        attrFactory = new AttributeFactory(md);
    }
    
    @Test
    public void testExtractNormalizedAttributes() {
        final Key metadata = new Key("shard", DATATYPE + "\0" + UID, "", new ColumnVisibility("U"), -1);
        
        String[][] testData = { {"FOO", "ABcd"}, {"abcd"}, {"FOO", "1234"}, {"1234"}, {"BAZ", "ABcd"}, {"ABcd"}, {"BAZ", "1234"}, {"+dE1.234"},
                {"BAR", "ABcd"}, {"abcd", "ABcd"}, {"BAR", "1234"}, {"1234", "+dE1.234"}};
        
        for (int i = 0; i < testData.length; i += 2) {
            String[] input = testData[i];
            String[] expected = testData[i + 1];
            Set<String> output = EventDataQueryExpressionVisitor.extractNormalizedAttributes(attrFactory, input[0], input[1], metadata);
            Set<String> missing = new TreeSet<String>();
            for (String s : expected) {
                if (!output.remove(s)) {
                    missing.add(s);
                }
            }
            
            StringBuilder b = new StringBuilder();
            if (!output.isEmpty()) {
                b.append(" Unexpected entries found: " + output.toString());
            }
            if (!missing.isEmpty()) {
                b.append(" Expected entries that were not found: " + output.toString());
            }
            
            if (b.length() > 0) {
                fail("Output did not match expected output for '" + input[0] + ":" + input[1] + "';" + b.toString());
            }
        }
    }
    
    @Test
    public void testExpressionFilterSingleWhitelist() {
        ExpressionFilter f = new ExpressionFilter(attrFactory, "FOO", true);
        f.addFieldValue("bar");
        
        Key k1 = createKey("FOO", "bar");
        Key k2 = createKey("FOO", "baz");
        Key k3 = createKey("FOO", "plover");
        Key k4 = createKey("BAR", "bar");
        
        assertTrue(f.apply(k1));
        assertFalse(f.apply(k2));
        assertFalse(f.apply(k3));
        assertFalse(f.apply(k4));
    }
    
    @Test
    public void testExpressionFilterMultiWhitelist() {
        ExpressionFilter f = new ExpressionFilter(attrFactory, "FOO", true);
        f.addFieldValue("bar");
        f.addFieldValue("baz");
        
        Key k1 = createKey("FOO", "bar");
        Key k2 = createKey("FOO", "baz");
        Key k3 = createKey("FOO", "plover");
        Key k4 = createKey("BAR", "bar");
        
        assertTrue(f.apply(k1));
        assertTrue(f.apply(k2));
        assertFalse(f.apply(k3));
        assertFalse(f.apply(k3));
        assertFalse(f.apply(k4));
    }
    
    @Test
    public void testExpressionFilterSingleBlacklist() {
        ExpressionFilter f = new ExpressionFilter(attrFactory, "FOO", false);
        f.addFieldValue("bar");
        
        Key k1 = createKey("FOO", "bar");
        Key k2 = createKey("FOO", "baz");
        Key k3 = createKey("FOO", "plover");
        Key k4 = createKey("BAR", "bar");
        
        assertFalse(f.apply(k1));
        assertTrue(f.apply(k2));
        assertTrue(f.apply(k3));
        assertTrue(f.apply(k4));
    }
    
    @Test
    public void testExpressionFilterMultiBlacklist() {
        ExpressionFilter f = new ExpressionFilter(attrFactory, "FOO", false);
        f.addFieldValue("bar");
        f.addFieldValue("baz");
        
        Key k1 = createKey("FOO", "bar");
        Key k2 = createKey("FOO", "baz");
        Key k3 = createKey("FOO", "plover");
        Key k4 = createKey("BAR", "bar");
        
        assertFalse(f.apply(k1));
        assertFalse(f.apply(k2));
        assertTrue(f.apply(k3));
        assertTrue(f.apply(k3));
        assertTrue(f.apply(k4));
    }
    
    @Test
    public void test() throws Exception {
        String originalQuery = "FOO == 'abc'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        assertNotNull(filter.get("FOO"));
        
        Key p1 = createKey("FOO", "abc");
        Key n1 = createKey("FOO", "def");
        Key n2 = createKey("BAR", "abc");
        
        assertNotNull(filter.get("FOO"));
        assertTrue(filter.get("FOO").apply(p1));
        assertFalse(filter.get("FOO").apply(n1));
        
        assertNull(filter.get("BAR"));
    }
    
    @Test
    public void testNegation() throws Exception {
        String originalQuery = "FOO != 'abc'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        assertNotNull(filter.get("FOO"));
        
        Key p1 = createKey("FOO", "abc");
        Key n1 = createKey("FOO", "def");
        
        assertNotNull(filter.get("FOO"));
        assertTrue(filter.get("FOO").apply(p1));
        assertFalse(filter.get("FOO").apply(n1));
        
        assertNull(filter.get("BAR"));
    }
    
    @Test
    public void testAndNegation() throws Exception {
        String originalQuery = "FOO != 'abc' && BAR == 'def'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key n1 = createKey("FOO", "def");
        Key p2 = createKey("BAR", "abc");
        Key n2 = createKey("BAR", "def");
        
        assertNotNull(filter.get("FOO"));
        assertTrue(filter.get("FOO").apply(p1));
        assertFalse(filter.get("FOO").apply(n1));
        
        assertNotNull(filter.get("BAR"));
        assertFalse(filter.get("BAR").apply(p2));
        assertTrue(filter.get("BAR").apply(n2));
    }
    
    @Test
    public void testAndNegationTwo() throws Exception {
        String originalQuery = "FOO == 'abc' && !(BAR == 'def')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key n1 = createKey("FOO", "def");
        Key p2 = createKey("BAR", "abc");
        Key n2 = createKey("BAR", "def");
        
        assertNotNull(filter.get("FOO"));
        assertTrue(filter.get("FOO").apply(p1));
        assertFalse(filter.get("FOO").apply(n1));
        
        assertNotNull(filter.get("BAR"));
        assertFalse(filter.get("BAR").apply(p2));
        assertTrue(filter.get("BAR").apply(n2));
    }
    
    @Test
    public void testOrSame() throws Exception {
        String originalQuery = "FOO == 'abc' || FOO == 'def'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "def");
        Key p3 = createKey("FOO", "ghi");
        
        assertNotNull(filter.get("FOO"));
        
        assertTrue(filter.get("FOO").apply(p1));
        assertTrue(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
        
        assertNull(filter.get("BAR"));
    }
    
    @Test
    public void testAndSame() throws Exception {
        String originalQuery = "FOO == 'abc' && FOO == 'def'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "def");
        Key p3 = createKey("FOO", "ghi");
        
        assertNotNull(filter.get("FOO"));
        
        assertTrue(filter.get("FOO").apply(p1));
        assertTrue(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
        
        assertNull(filter.get("BAR"));
    }
    
    @Test
    public void testAndSameInvert() throws Exception {
        String originalQuery = "'abc' == FOO && 'def' == FOO";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "def");
        Key p3 = createKey("FOO", "ghi");
        
        assertNotNull(filter.get("FOO"));
        
        assertTrue(filter.get("FOO").apply(p1));
        assertTrue(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
        
        assertNull(filter.get("BAR"));
    }
    
    @Test
    public void testAndNotInvert() throws Exception {
        String originalQuery = "'abc' == FOO and not ('def' == FOO)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "def");
        Key p3 = createKey("FOO", "ghi");
        
        assertNotNull(filter.get("FOO"));
        
        assertTrue(filter.get("FOO").apply(p1));
        assertTrue(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
        
        assertNull(filter.get("BAR"));
    }
    
    @Test
    public void testRange1() throws Exception {
        String originalQuery = "BAZ >= '+aE5' AND BAZ <= '+bE1.2'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        printJexlScript(script);
        
        Key p1 = createKey("BAZ", "4");
        Key p2 = createKey("BAZ", "6");
        Key p3 = createKey("BAZ", "15");
        Key p4 = createKey("FOO", "6");
        
        assertNotNull(filter.get("BAZ"));
        
        assertFalse(filter.get("BAZ").apply(p1));
        assertTrue(filter.get("BAZ").apply(p2));
        assertFalse(filter.get("BAZ").apply(p3));
        assertFalse(filter.get("BAZ").apply(p4));
        
        assertNull(filter.get("FOO"));
    }
    
    @Test
    public void testRange2() throws Exception {
        // Between 5 and 12.
        String originalQuery = "FOO == 'abc' AND (BAZ >= '+aE5' AND BAZ <= '+bE1.2')";
        // @TODO, use ExpandMultiNormalizedTerms to normalize this query?
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        printJexlScript(script);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "def");
        Key p3 = createKey("FOO", "ghi");
        Key b1 = createKey("BAZ", "4");
        Key b2 = createKey("BAZ", "6");
        Key b3 = createKey("BAZ", "15");
        
        assertNotNull(filter.get("FOO"));
        
        assertTrue(filter.get("FOO").apply(p1));
        assertFalse(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
        
        assertFalse(filter.get("BAZ").apply(b1));
        assertTrue(filter.get("BAZ").apply(b2));
        assertFalse(filter.get("BAZ").apply(b3));
        
        assertNull(filter.get("BAR"));
    }
    
    @Ignore
    // TODO: will we ever be able to get this to work?
    public void testRangeFunction() throws Exception {
        String originalQuery = "f:between(BAZ,5,12)";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        printJexlScript(script);
        
        Key p1 = createKey("BAZ", "6");
        Key p2 = createKey("BAZ", "1");
        Key p3 = createKey("BAZ", "13");
        
        assertNotNull(filter.get("BAZ"));
        
        assertTrue(filter.get("BAZ").apply(p1));
        assertTrue(filter.get("BAZ").apply(p2));
        assertFalse(filter.get("BAZ").apply(p3));
        
        assertNull(filter.get("BAR"));
    }
    
    @Test
    public void testAndSameGroupingOnKey() throws Exception {
        String originalQuery = "FOO == 'abc' && FOO == 'def'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO.1", "abc");
        Key p2 = createKey("FOO.2", "def");
        Key p3 = createKey("FOO.3", "ghi");
        
        assertNotNull(filter.get("FOO"));
        
        assertTrue(filter.get("FOO").apply(p1));
        assertTrue(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
        
        assertNull(filter.get("BAR"));
    }
    
    @Ignore
    // TODO: This may never happen - e.g: function expansion has happened by now.
    public void testAndSameGroupingOnQuery() throws Exception {
        String originalQuery = "FOO.1 == 'abc' && FOO.2 == 'def'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "def");
        Key p3 = createKey("FOO", "ghi");
        
        assertNotNull(filter.get("FOO"));
        
        assertTrue(filter.get("FOO").apply(p1));
        assertTrue(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
        
        assertNull(filter.get("BAR"));
    }
    
    @Ignore
    // TODO: This may never happen - e.g: function expansion has happened by now.
    public void testAndSameGroupingOnBoth() throws Exception {
        String originalQuery = "FOO.1 == 'abc' && FOO.2 == 'def'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "def");
        Key p3 = createKey("FOO", "ghi");
        
        assertNotNull(filter.get("FOO"));
        
        assertTrue(filter.get("FOO.1").apply(p1));
        assertTrue(filter.get("FOO.2").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
        
        assertNull(filter.get("BAR"));
    }
    
    @Test
    public void testMultiple() throws Exception {
        String originalQuery = "FOO == 'abc' || BAR == 'def'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        assertNotNull(filter.get("FOO"));
        assertNotNull(filter.get("BAR"));
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "def");
        Key p3 = createKey("FOO", "ghi");
        
        Key n1 = createKey("BAR", "abc");
        Key n2 = createKey("BAR", "def");
        Key n3 = createKey("BAR", "ghi");
        
        assertNotNull(filter.get("FOO"));
        
        assertTrue(filter.get("FOO").apply(p1));
        assertFalse(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
        
        assertNotNull(filter.get("BAR"));
        assertFalse(filter.get("BAR").apply(n1));
        assertTrue(filter.get("BAR").apply(n2));
        assertFalse(filter.get("BAR").apply(n3));
        
        // confusion - the filter is for FOO, but the values match - a whitelist should fail.
        assertFalse(filter.get("BAR").apply(p2));
    }
    
    @Test
    public void testRegexHead() throws Exception {
        String originalQuery = "FOO =~ 'abc.*'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "abc123");
        Key p3 = createKey("FOO", "1abc3");
        
        assertTrue(filter.get("FOO").apply(p1));
        assertTrue(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
    }
    
    @Test
    public void testRegexHeadTail() throws Exception {
        String originalQuery = "FOO =~ '.*abc'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "123abc");
        Key p3 = createKey("FOO", "1abc3");
        
        assertTrue(filter.get("FOO").apply(p1));
        assertTrue(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
    }
    
    @Test
    public void testRegexHeadTailNegation() throws Exception {
        String originalQuery = "FOO !~ '.*abc'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        final Map<String,ExpressionFilter> filter = EventDataQueryExpressionVisitor.getExpressionFilters(script, attrFactory);
        
        Key p1 = createKey("FOO", "abc");
        Key p2 = createKey("FOO", "123abc");
        Key p3 = createKey("FOO", "1abc3");
        
        assertTrue(filter.get("FOO").apply(p1));
        assertTrue(filter.get("FOO").apply(p2));
        assertFalse(filter.get("FOO").apply(p3));
    }
    
    public static void printJexlScript(ASTJexlScript script) {
        PrintingVisitor v = new PrintingVisitor();
        script.jjtAccept(v, "");
    }
    
    MockMetadataHelper helper = new MockMetadataHelper();
    MockDateIndexHelper helper2 = new MockDateIndexHelper();
    ShardQueryConfiguration config = new ShardQueryConfiguration();
    
    @Test
    public void testExpandedFunctionQuery() throws Exception {
        
        Set<String> contentFields = new HashSet<String>();
        contentFields.add("FOO");
        contentFields.add("BAR");
        
        helper.addTermFrequencyFields(contentFields);
        helper.setIndexedFields(contentFields);
        
        String originalQuery = "content:phrase(termOffsetMap, 'abc', 'def')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2, script);
        String newQuery = JexlStringBuildingVisitor.buildQuery(newScript);
        System.out.println(newQuery);
    }
    
    public static Key createKey(String fieldName, String fieldValue) {
        return createKey("20170827_1", DATATYPE, UID, fieldName, fieldValue, cv1);
    }
    
    public static Key createKey(String rowId, String datatype, String uid, String fieldName, String fieldValue, ColumnVisibility vis) {
        return new Key(rowId, datatype + '\0' + uid, fieldName + '\0' + fieldValue, vis, 0L);
    }
}
