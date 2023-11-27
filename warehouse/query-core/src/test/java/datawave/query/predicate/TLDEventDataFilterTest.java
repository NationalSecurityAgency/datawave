package datawave.query.predicate;

import static datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.getExpressionFilters;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.ExpressionFilter;
import datawave.query.jexl.visitors.FunctionIndexQueryExpansionVisitor;
import datawave.query.util.MockDateIndexHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.query.util.TypeMetadata;

public class TLDEventDataFilterTest extends EasyMockSupport {
    private TLDEventDataFilter filter;
    private ASTJexlScript mockScript;
    private TypeMetadata mockTypeMetadata;

    private final MockMetadataHelper helper = new MockMetadataHelper();
    private final MockDateIndexHelper helper2 = new MockDateIndexHelper();
    private final ShardQueryConfiguration config = new ShardQueryConfiguration();

    @Before
    public void setup() {
        mockScript = createMock(ASTJexlScript.class);
        mockTypeMetadata = createMock(TypeMetadata.class);

        String lcNoDiacritics = LcNoDiacriticsType.class.getName();
        String number = NumberType.class.getName();

        TypeMetadata md = new TypeMetadata();

        md.put("FOO", "datatype", lcNoDiacritics);
        md.put("FOO2", "datatype", lcNoDiacritics);
        md.put("BAZ", "datatype", number);
        md.put("BAR", "datatype", lcNoDiacritics);
        md.put("BAR", "datatype", number);
    }

    @Test
    public void getCurrentField_standardTest() {
        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        // expected key structure
        Key key = new Key("row", "column", "FIELD" + Constants.NULL_BYTE_STRING + "value");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, -1, -1);
        String field = filter.getCurrentField(key);

        assertEquals("FIELD", field);

        verifyAll();
    }

    @Test
    public void getCurrentField_groupingTest() {
        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        // expected key structure
        Key key = new Key("row", "column", "FIELD.part_1.part_2.part_3" + Constants.NULL_BYTE_STRING + "value");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, -1, -1);
        String field = filter.getCurrentField(key);

        assertEquals("FIELD", field);

        verifyAll();
    }

    @Test
    public void getCurrentField_fiTest() {
        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        // expected key structure
        Key key = new Key("row", "fi" + Constants.NULL + "FIELD", "value" + Constants.NULL + "datatype" + Constants.NULL + "123.345.456");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, -1, -1);
        String field = filter.getCurrentField(key);

        assertEquals("FIELD", field);

        verifyAll();
    }

    @Test
    public void getCurrentField_fiGroupingTest() {
        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        // expected key structure
        Key key = new Key("row", "fi" + Constants.NULL + "FIELD.name", "value" + Constants.NULL + "datatype" + Constants.NULL + "123.345.456");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, -1, -1);
        String field = filter.getCurrentField(key);

        assertEquals("FIELD", field);

        verifyAll();
    }

    @Test
    public void getCurrentField_tfTest() {
        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        // expected key structure
        Key key = new Key("row", "tf", "datatype" + Constants.NULL + "123.234.345" + Constants.NULL + "value" + Constants.NULL + "FIELD");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, -1, -1);
        String field = filter.getCurrentField(key);

        assertEquals("FIELD", field);

        verifyAll();
    }

    @Test
    public void getCurrentField_tfGroupingTest() {
        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);
        replayAll();

        // expected key structure
        Key key = new Key("row", "tf", "datatype" + Constants.NULL + "123.234.345" + Constants.NULL + "value" + Constants.NULL + "FIELD.name");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, -1, -1);
        String field = filter.getCurrentField(key);

        assertEquals("FIELD", field);

        verifyAll();
    }

    @Test
    public void keep_emptyMapTest() {
        Multimap<Integer,String> fieldLimits = HashMultimap.create();

        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        // expected key structure
        Key key = new Key("row", "column", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, 1, -1, fieldLimits, "LIMIT_FIELD",
                        Collections.emptySet());

        assertTrue(filter.keep(key));
        assertNull(filter.getSeekRange(key, key.followingKey(PartialKey.ROW), false));

        verifyAll();
    }

    @Test
    public void keep_anyFieldTest() throws ParseException {
        Multimap<Integer,String> fieldLimits = HashMultimap.create();
        fieldLimits.put(1, Constants.ANY_FIELD);

        Set<String> disallowlist = new HashSet<>();
        disallowlist.add("FIELD3");

        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FIELD2 == 'bar'");

        replayAll();

        // expected key structure
        Key key = new Key("row", "datatype" + Constants.NULL + "123.345.456", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(query, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(query, Collections.singleton("FIELD2"), expressionFilters, null, disallowlist, 1, -1, fieldLimits, "LIMIT_FIELD",
                        Collections.emptySet());

        assertTrue(filter.keep(key));
        // increments counts = 1
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<>(key, null)));
        assertNull(filter.getSeekRange(key, key.followingKey(PartialKey.ROW), false));
        // does not increment counts so will still return true
        assertTrue(filter.keep(key));
        // increment counts = 2 so rejects based on field filter
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<>(key, null)));
        Range seekRange = filter.getSeekRange(key, key.followingKey(PartialKey.ROW), false);
        assertNotNull(seekRange);
        assertEquals(seekRange.getStartKey().getRow(), key.getRow());
        assertEquals(seekRange.getStartKey().getColumnFamily(), key.getColumnFamily());
        assertEquals(seekRange.getStartKey().getColumnQualifier().toString(), "FIELD1" + "\u0001");
        assertTrue(seekRange.isStartKeyInclusive());

        // now fails
        assertFalse(filter.keep(key));

        verifyAll();
    }

    @Test
    public void keep_limitFieldTest() {
        Multimap<Integer,String> fieldLimits = HashMultimap.create();
        fieldLimits.put(1, "FIELD1");

        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        Key key1 = new Key("row", "column", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        Key key2 = new Key("row", "column", "FIELD2" + Constants.NULL_BYTE_STRING + "value");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, 1, -1, fieldLimits, "LIMIT_FIELD",
                        Collections.emptySet());

        assertTrue(filter.keep(key1));
        // increments counts = 1
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<>(key1, null)));
        assertNull(filter.transform(key1));
        assertNull(filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false));
        // does not increment counts so will still return true
        assertTrue(filter.keep(key1));
        // increments counts = 2
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<>(key1, null)));
        Range seekRange = filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false);
        assertNotNull(seekRange);
        assertEquals(seekRange.getStartKey().getRow(), key1.getRow());
        assertEquals(seekRange.getStartKey().getColumnFamily(), key1.getColumnFamily());
        assertEquals(seekRange.getStartKey().getColumnQualifier().toString(), "FIELD1" + "\u0001");
        assertTrue(seekRange.isStartKeyInclusive());
        // now fails
        assertFalse(filter.keep(key1));

        Key limitKey = filter.transform(key1);
        assertNotNull(limitKey);
        assertEquals(limitKey.getRow(), key1.getRow());
        assertEquals(limitKey.getColumnFamily(), key1.getColumnFamily());
        assertEquals(limitKey.getColumnQualifier().toString(), "LIMIT_FIELD" + Constants.NULL + "FIELD1");

        // unlimited field
        assertTrue(filter.keep(key2));
        // increments counts = 1
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<>(key2, null)));
        assertNull(filter.transform(key2));
        assertNull(filter.getSeekRange(key2, key2.followingKey(PartialKey.ROW), false));

        assertTrue(filter.keep(key2));
        // increments counts = 2
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<>(key2, null)));
        assertNull(filter.getSeekRange(key2, key2.followingKey(PartialKey.ROW), false));
        // still passes
        assertTrue(filter.keep(key2));
        assertNull(filter.transform(key2));

        verifyAll();
    }

    @Test
    public void getSeekRange_maxFieldSeekNotEqualToLimit() {
        Multimap<Integer,String> fieldLimits = HashMultimap.create();
        fieldLimits.put(1, "FIELD1");

        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        Key key1 = new Key("row", "column", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        Key key2 = new Key("row", "column", "FIELD2" + Constants.NULL_BYTE_STRING + "value");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, 3, -1, fieldLimits, "LIMIT_FIELD",
                        Collections.emptySet());

        assertTrue(filter.keep(key1));
        // increments counts = 1
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<>(key1, null)));
        assertNull(filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false));
        // does not increment counts so will still return true
        assertTrue(filter.keep(key1));
        // increment counts = 2 rejected by field count
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<>(key1, null)));
        assertNull(filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false));

        // now fails
        assertFalse(filter.keep(key1));

        // see another key on apply to trigger the seek range
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<>(key1, null)));
        Range seekRange = filter.getSeekRange(key1, key1.followingKey(PartialKey.ROW), false);
        assertNotNull(seekRange);
        assertEquals(seekRange.getStartKey().getRow(), key1.getRow());
        assertEquals(seekRange.getStartKey().getColumnFamily(), key1.getColumnFamily());
        assertEquals(seekRange.getStartKey().getColumnQualifier().toString(), "FIELD1" + "\u0001");
        assertTrue(seekRange.isStartKeyInclusive());

        verifyAll();
    }

    @Test
    public void getParseInfo_isRootTest() {
        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        // expected key structure
        Key key = new Key("row", "datatype" + Constants.NULL + "123.234.345", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, -1, -1);

        TLDEventDataFilter.ParseInfo info = filter.getParseInfo(key);
        assertNotNull(info);
        assertEquals("FIELD1", info.getField());
        assertTrue(info.isRoot());

        // first two calls are made without the internal update to the cached parseInfo so are calculated independently

        key = new Key("row", "datatype" + Constants.NULL + "123.234.345", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        info = filter.getParseInfo(key);
        assertNotNull(info);
        assertEquals("FIELD1", info.getField());
        assertTrue(info.isRoot());

        key = new Key("row", "datatype" + Constants.NULL + "123.234.345.1", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        info = filter.getParseInfo(key);
        assertNotNull(info);
        assertEquals("FIELD1", info.getField());
        // this was wrong assumption based when fixed length UID parse assumptions were being made in the TLDEventDataFilter
        assertTrue(info.isRoot());

        key = new Key("row", "datatype" + Constants.NULL + "123.234.345", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        // use the keep method to set the previous call state
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertNotNull(info);
        assertEquals("FIELD1", info.getField());
        assertTrue(info.isRoot());

        // now test the child and see that it is not root
        key = new Key("row", "datatype" + Constants.NULL + "123.234.345.1", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertNotNull(info);
        assertEquals("FIELD1", info.getField());
        assertFalse(info.isRoot());

        // a second child
        key = new Key("row", "datatype" + Constants.NULL + "123.234.345.2", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertNotNull(info);
        assertEquals("FIELD1", info.getField());
        assertFalse(info.isRoot());

        // a longer child
        key = new Key("row", "datatype" + Constants.NULL + "123.234.345.23", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertNotNull(info);
        assertEquals("FIELD1", info.getField());
        assertFalse(info.isRoot());

        // jump back to the original
        key = new Key("row", "datatype" + Constants.NULL + "123.234.345", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        filter.keep(key);
        info = filter.getParseInfo(key);
        assertNotNull(info);
        assertEquals("FIELD1", info.getField());
        assertTrue(info.isRoot());

        verifyAll();
    }

    @Test
    public void setDocumentClearParseInfoTest() {
        expect(mockScript.jjtGetNumChildren()).andReturn(0).anyTimes();
        expect(mockScript.jjtAccept(isA(EventDataQueryExpressionVisitor.class), eq(""))).andReturn(null);

        replayAll();

        // expected key structure
        Key key1 = new Key("row", "datatype" + Constants.NULL + "123.234.345", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        Key key2 = new Key("row", "datatype" + Constants.NULL + "123.234.345.1", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        Key key3 = new Key("row", "datatype" + Constants.NULL + "123.234.34567", "FIELD1" + Constants.NULL_BYTE_STRING + "value");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(mockScript, Collections.singleton("FIELD"), expressionFilters, null, null, -1, -1);

        filter.startNewDocument(key1);
        // set the lastParseInfo to this key
        filter.keep(key1);
        assertFalse(filter.getParseInfo(key2).isRoot());
        filter.keep(key2);
        // breaking contract calling this on a new document without calling set document, do this to illustrate the potential problem
        assertFalse(filter.getParseInfo(key3).isRoot());

        // property follow the contract by setting the context for the document first
        filter.startNewDocument(key2);
        assertTrue(filter.getParseInfo(key2).isRoot());

        verifyAll();
    }

    @Test
    public void apply_acceptSuperRejectTest() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == 'bar'");

        expect(mockTypeMetadata.getTypeMetadata("FOO", "datatype")).andReturn(Collections.emptyList());

        replayAll();

        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(query, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(query, Collections.singleton("FOO"), expressionFilters, null, null, -1, 1);

        Key key1 = new Key("row", "datatype" + Constants.NULL + "123.234.345", "FOO" + Constants.NULL_BYTE_STRING + "baz");
        Key key2 = new Key("row", "datatype" + Constants.NULL + "123.234.345.11", "FOO" + Constants.NULL_BYTE_STRING + "baz");
        boolean tldResult = filter.apply(new AbstractMap.SimpleEntry<>(key1, null));
        boolean result = filter.apply(new AbstractMap.SimpleEntry<>(key2, null));

        verifyAll();

        assertTrue(tldResult);
        assertFalse(result);
    }

    @Test
    public void apply_acceptSuperRejectChildTest() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == 'bar'");

        expect(mockTypeMetadata.getTypeMetadata("FOO", "datatype")).andReturn(Collections.emptyList());

        replayAll();

        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(query, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(query, Collections.singleton("FOO"), expressionFilters, null, null, -1, 1);

        // process parent first to set up proper root calculations
        Key parent = new Key("row", "datatype" + Constants.NULL + "123.234.345", "BAR" + Constants.NULL_BYTE_STRING + "baz");
        filter.keep(parent);
        filter.apply(new AbstractMap.SimpleEntry<>(parent, null));

        Key child = new Key("row", "datatype" + Constants.NULL + "123.234.345.11", "FOO" + Constants.NULL_BYTE_STRING + "baz");
        boolean result = filter.apply(new AbstractMap.SimpleEntry<>(child, null));

        verifyAll();

        assertFalse(result);
    }

    @Test
    public void functional_ContentExpansionTest() throws ParseException {
        Set<String> contentFields = new HashSet<>();
        contentFields.add("FOO");
        contentFields.add("BAR");

        helper.addTermFrequencyFields(contentFields);
        helper.setIndexedFields(contentFields);

        String originalQuery = "content:phrase(termOffsetMap, 'abc', 'def')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2, script);

        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(newScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(newScript, Sets.newHashSet("FOO", "BAR"), expressionFilters, null, null, -1, 1);

        assertTrue(filter.queryFields.contains("FOO"));
        assertTrue(filter.queryFields.contains("BAR"));
    }

    @Test
    public void functional_IncludesExpansionTest() throws ParseException {
        Set<String> contentFields = new HashSet<>();
        contentFields.add("FOO");
        contentFields.add("BAR");

        helper.addTermFrequencyFields(contentFields);
        helper.setIndexedFields(contentFields);

        String originalQuery = "filter:includeRegex(FOO, '.*23ab.*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2, script);

        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(mockScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(newScript, Sets.newHashSet("FOO", "BAR"), expressionFilters, null, null, -1, 1);

        assertTrue(filter.queryFields.contains("FOO"));
    }

    @Test
    public void functionalIncludesExpansionTest() throws ParseException {
        Set<String> contentFields = new HashSet<>();
        contentFields.add("FOO");
        contentFields.add("BAR");

        helper.addTermFrequencyFields(contentFields);
        helper.setIndexedFields(contentFields);

        String originalQuery = "filter:excludeRegex(BAR, '.*23ab.*')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2, script);

        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(newScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(newScript, Sets.newHashSet("FOO", "BAR"), expressionFilters, null, null, -1, 1);

        assertTrue(filter.queryFields.contains("BAR"));
    }

    @Test
    public void functionalOccurrenceExpansionTest() throws ParseException {
        Set<String> contentFields = new HashSet<>();
        contentFields.add("FOO");
        contentFields.add("BAR");

        helper.addTermFrequencyFields(contentFields);
        helper.setIndexedFields(contentFields);

        String originalQuery = "filter:occurrence(FOO, '.*23ab.*') && BAR == '123'";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(originalQuery);
        ASTJexlScript newScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, helper, helper2, script);

        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(newScript, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(newScript, Sets.newHashSet("FOO", "BAR"), expressionFilters, null, null, -1, 1);

        assertTrue(filter.queryFields.contains("BAR"));
        assertTrue(filter.queryFields.contains("FOO"));
    }

    @Test
    public void keep_nonEventApplyBypass() throws ParseException {
        Multimap<Integer,String> fieldLimits = HashMultimap.create();
        fieldLimits.put(1, Constants.ANY_FIELD);

        Set<String> disallowlist = new HashSet<>();
        disallowlist.add("FIELD3");

        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FIELD2 == 'bar'");

        Set<String> nonEventFields = new HashSet<>();
        nonEventFields.add("FIELD2");

        expect(mockTypeMetadata.getTypeMetadata("FIELD2", "datatype")).andReturn(Collections.emptyList()).anyTimes();

        replayAll();

        // disallowlisted tld key to initialize doc
        Key rootKey = new Key("row", "datatype" + Constants.NULL + "123.345.456", "FIELD3" + Constants.NULL_BYTE_STRING + "value");
        // child key that would normally not be kept
        Key key = new Key("row", "datatype" + Constants.NULL + "123.345.456.1", "FIELD2" + Constants.NULL_BYTE_STRING + "bar");
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(query, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(query, Collections.singleton("FIELD2"), expressionFilters, null, disallowlist, 1, -1, fieldLimits, "LIMIT_FIELD",
                        nonEventFields);

        // set the parse info correctly
        filter.startNewDocument(rootKey);
        filter.keep(rootKey);

        // test the key, would normally have been rejected, but as a child with a non-event that matches it should be kept
        assertTrue(filter.keep(key));

        verifyAll();
    }

    @Test
    public void apply_acceptGroupingFields() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("grouping:matchesInGroup(FOO, 'bar')");
        expect(mockTypeMetadata.getTypeMetadata("FOO", "datatype")).andReturn(Collections.emptyList()).anyTimes();

        replayAll();

        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(query, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(query, Collections.singleton("FOO"), expressionFilters, null, null, -1, -1);

        Key key1 = new Key("row", "datatype" + Constants.NULL + "123.234.345", "FOO" + Constants.NULL_BYTE_STRING + "baz");
        Key key2 = new Key("row", "datatype" + Constants.NULL + "123.234.345.11", "FOO" + Constants.NULL_BYTE_STRING + "baz");
        Key key3 = new Key("row", "datatype" + Constants.NULL + "123.234.345.11", "FOOT" + Constants.NULL_BYTE_STRING + "bar");
        Key key4 = new Key("row", "datatype" + Constants.NULL + "123.234.345.11", "FOO" + Constants.NULL_BYTE_STRING + "bar");

        boolean result1 = filter.apply(new AbstractMap.SimpleEntry<>(key1, null));
        boolean result2 = filter.apply(new AbstractMap.SimpleEntry<>(key2, null));
        boolean result3 = filter.apply(new AbstractMap.SimpleEntry<>(key3, null));
        boolean result4 = filter.apply(new AbstractMap.SimpleEntry<>(key4, null));

        verifyAll();

        assertTrue(result1);
        assertFalse(result2);
        assertFalse(result3);
        assertTrue(result4);
    }

    @Test
    public void testBadIdentifier() throws ParseException {
        String query = "content:phrase(FOO, termOffsetMap, 'bar', 'baz') && FOO == 'bar' && FOO == 'baz' && Foo2 == 'bad'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        expect(mockTypeMetadata.getTypeMetadata("FOO", "datatype")).andReturn(Collections.emptyList()).anyTimes();
        replayAll();

        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(script, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(script, Sets.newHashSet("FOO", "FOO2"), expressionFilters, null, null, -1, -1);

        // asserts that 'termOffsetMap' is not considered a query field
        // asserts that malformed field 'Foo2' is not considered a query field
        assertEquals(Collections.singletonList("FOO"), filter.queryFields);
    }

    @Test
    public void testGetSeekRangeForChildWithMultipleValuesPerField() throws ParseException {
        String query = "FOO == 'bar' && FOO2 == 'value3'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        expect(mockTypeMetadata.getTypeMetadata("FOO2", "datatype")).andReturn(Collections.emptyList()).anyTimes();
        replayAll();

        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(script, new AttributeFactory(mockTypeMetadata));
        filter = new TLDEventDataFilter(script, Sets.newHashSet("FOO", "FOO2"), expressionFilters, null, null, 1, 1);

        Key k1 = new Key("row", "datatype\u0000d8zay2.-3pnndm.-anolok", "FOO\0bar");
        Key k2 = new Key("row", "datatype\u0000d8zay2.-3pnndm.-anolok.1", "FOO2\0value1");
        Key k3 = new Key("row", "datatype\u0000d8zay2.-3pnndm.-anolok.2", "FOO2\0value2");
        Key k4 = new Key("row", "datatype\u0000d8zay2.-3pnndm.-anolok.3", "FOO2\0value3");

        // TLD field is applied, kept, no seek range generated
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<>(k1, null)));
        assertTrue(filter.keep(k1));
        assertNull(filter.getSeekRange(k1, null, false));

        // child field is not applied, is not kept, no seek range generated
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<>(k2, null)));
        assertFalse(filter.keep(k2));
        assertNull(filter.getSeekRange(k2, null, false));

        // child field is not applied, is not kept, no seek range generated
        assertFalse(filter.apply(new AbstractMap.SimpleEntry<>(k3, null)));
        assertFalse(filter.keep(k3));
        assertNull(filter.getSeekRange(k3, null, false));

        // child field IS applied, IS kept, no seek range generated
        assertTrue(filter.apply(new AbstractMap.SimpleEntry<>(k4, null)));
        assertFalse(filter.keep(k4));
        assertNull(filter.getSeekRange(k4, null, false));
    }
}
