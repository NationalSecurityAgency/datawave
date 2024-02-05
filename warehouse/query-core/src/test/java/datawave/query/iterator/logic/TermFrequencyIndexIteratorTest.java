package datawave.query.iterator.logic;

import static datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.ExpressionFilter;
import static datawave.query.jexl.visitors.EventDataQueryExpressionVisitor.getExpressionFilters;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.predicate.EventDataQueryExpressionFilter;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.TLDEventDataFilter;
import datawave.query.predicate.TimeFilter;
import datawave.query.tld.TLDTermFrequencyAggregator;
import datawave.query.util.TypeMetadata;

public class TermFrequencyIndexIteratorTest {

    private SortedKeyValueIterator source;
    private TypeMetadata typeMetadata;
    private TermFrequencyAggregator aggregator;
    private Set<String> fieldsToKeep;
    private EventDataQueryFilter filter;

    @Before
    public void setup() throws ParseException {
        List<Map.Entry<Key,Value>> baseSource = new ArrayList<>();
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("row", "type1", "123.345.456", "FOO", "bar"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("row", "type1", "123.345.456", "FOO", "baz"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("row", "type1", "123.345.456.1", "FOO", "buf"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("row", "type1", "123.345.456.1", "FOO", "buz"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("row", "type1", "123.345.456.2", "FOO", "alf"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("row", "type1", "123.345.456.2", "FOO", "arm"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("row", "type1", "123.345.456.2", "FOOT", "armfoot"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("row", "type1", "123.345.456.3", "AFOO", "alfa"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("row", "type1", "123.345.456.3", "ZFOO", "alfz"), new Value()));

        source = new SortedListKeyValueIterator(baseSource);

        String lcNoDiacritics = LcNoDiacriticsType.class.getName();

        typeMetadata = new TypeMetadata();
        typeMetadata.put("FOO", "type1", lcNoDiacritics);
        typeMetadata.put("FOO", "datatype", lcNoDiacritics);

        fieldsToKeep = new HashSet<>();
        fieldsToKeep.add("FOO");

        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='buz' || FOO=='alf' || FOO=='arm'");
        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(script, attributeFactory);

        filter = new EventDataQueryExpressionFilter(expressionFilters);

        aggregator = new TermFrequencyAggregator(fieldsToKeep, filter);
    }

    @Test
    public void testEmptyRange() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "biz"), true, getFiKey("row", "type1", "123.345.456", "FOO", "bzz"), true);
        TermFrequencyAggregator aggregator = new TermFrequencyAggregator(null, null);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertFalse(iterator.hasTop());
    }

    @Test
    public void testScanMinorRange() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "baz"), true, getFiKey("row", "type1", "123.345.456", "FOO", "baz"), true);
        TermFrequencyAggregator aggregator = new TermFrequencyAggregator(null, null);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertTrue(iterator.hasTop());
        Document d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        assertTrue((d.getDictionary().get("FOO").getData()).equals("baz"));
    }

    @Test
    public void testScanMinorRangeTLD() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "baz"), true, getFiKey("row", "type1", "123.345.456", "FOO", "baz"), true);
        TermFrequencyAggregator aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertTrue(iterator.hasTop());
        Document d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        assertTrue((d.getDictionary().get("FOO").getData()).equals("baz"));
    }

    @Test
    public void testScanPartialRanges() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "bar"), false);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertTrue(iterator.hasTop());
        Document d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() + "", d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        assertTrue((d.getDictionary().get("FOO").getData()).equals("arm"));
    }

    @Test
    public void testScanPartialRangesTLD() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "bar"), false);
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertTrue(iterator.hasTop());
        Document d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() + "", d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        assertTrue((d.getDictionary().get("FOO").getData()).equals("arm"));
    }

    @Test
    public void testScanFullRange() throws IOException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), true, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), true);

        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertTrue(iterator.hasTop());
        Document d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        assertTrue(((Set) d.getDictionary().get("FOO").getData()).size() == 2);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        assertTrue(i.next().getValue().equals("bar"));
        assertTrue(i.next().getValue().equals("baz"));

        iterator.next();

        assertTrue(iterator.hasTop());
        d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        assertTrue(((Set) d.getDictionary().get("FOO").getData()).size() == 2);
        i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        assertTrue(i.next().getValue().equals("buf"));
        assertTrue(i.next().getValue().equals("buz"));

        iterator.next();

        assertTrue(iterator.hasTop());
        d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        assertTrue(((Set) d.getDictionary().get("FOO").getData()).size() == 2);
        i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        assertTrue(i.next().getValue().equals("alf"));
        assertTrue(i.next().getValue().equals("arm"));
    }

    @Test
    public void testScanFullRangeTLD() throws IOException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), true, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), true);

        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertTrue(iterator.hasTop());
        Document d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        assertTrue(((Set) d.getDictionary().get("FOO").getData()).size() == 6);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        assertTrue(i.next().getValue().equals("bar"));
        assertTrue(i.next().getValue().equals("baz"));
        assertTrue(i.next().getValue().equals("buf"));
        assertTrue(i.next().getValue().equals("buz"));
        assertTrue(i.next().getValue().equals("alf"));
        assertTrue(i.next().getValue().equals("arm"));

        iterator.next();
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testEndingFieldMismatch() throws IOException, ParseException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456.3", "FOO", "alf"), true,
                        getFiKey("row", "type1", "123.345.456.3", Constants.MAX_UNICODE_STRING, "buz"), false);

        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'");
        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(script, attributeFactory);

        filter = new EventDataQueryExpressionFilter(expressionFilters);
        aggregator = new TermFrequencyAggregator(fieldsToKeep, filter);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testScanFullRangeExclusive() throws IOException, ParseException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), false);

        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'");
        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(script, attributeFactory);

        filter = new EventDataQueryExpressionFilter(expressionFilters);
        aggregator = new TermFrequencyAggregator(fieldsToKeep, filter);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertTrue(iterator.hasTop());
        Document d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        assertTrue(i.next().getValue().equals("bar"));
        assertTrue(i.next().getValue().equals("baz"));

        iterator.next();

        assertTrue(iterator.hasTop());
        d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        assertTrue(i.next().getValue().equals("buf"));
        assertTrue(i.next().getValue().equals("buz"));

        iterator.next();

        assertTrue(iterator.hasTop());
        d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        assertTrue(d.getDictionary().get("FOO").getData().equals("arm"));

        iterator.next();
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testScanFullRangeExclusiveTLD() throws IOException, ParseException {
        String query = "FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);

        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), false);

        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(script, attributeFactory);
        filter = new TLDEventDataFilter(script, Collections.singleton("FOO"), expressionFilters, null, null, -1, -1, HashMultimap.create(), null, fieldsToKeep);

        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertTrue(iterator.hasTop());
        Document d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        assertTrue(i.next().getValue().equals("bar"));
        assertTrue(i.next().getValue().equals("baz"));
        assertTrue(i.next().getValue().equals("buf"));
        assertTrue(i.next().getValue().equals("arm"));

        iterator.next();
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testScanFullRangeExclusiveEventDataQueryExpressionFilter() throws IOException, ParseException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), false);

        ASTJexlScript script = JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'");
        AttributeFactory attributeFactory = new AttributeFactory(typeMetadata);
        Map<String,ExpressionFilter> expressionFilters = getExpressionFilters(script, attributeFactory);

        filter = new EventDataQueryExpressionFilter(expressionFilters);
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, aggregator);

        // jump to the first doc
        iterator.seek(null, null, true);

        assertTrue(iterator.hasTop());
        Document d = iterator.document();
        assertTrue(d != null);
        assertTrue(d.getDictionary().size() == 2);
        assertTrue(d.getDictionary().get("FOO") != null);
        assertTrue(d.getDictionary().get("RECORD_ID") != null);
        assertTrue(d.getDictionary().get("FOO").getData() != null);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        assertTrue(i.next().getValue().equals("bar"));
        assertTrue(i.next().getValue().equals("baz"));
        assertTrue(i.next().getValue().equals("buf"));
        assertTrue(i.next().getValue().equals("buz"));
        assertTrue(i.next().getValue().equals("alf"));
        assertTrue(i.next().getValue().equals("arm"));

        iterator.next();
        assertFalse(iterator.hasTop());
    }

    @Test
    public void testSingleDocId() {
        SortedListKeyValueIterator source = getIterator();

        Key start = new Key("row", "fi\0FOO", "value1\u0000datatype\u00001");
        Key end = new Key("row", "fi\0FOO", "value1\uffff\u0000datatype\u00001");

        TermFrequencyIndexIterator tfIter = new TermFrequencyIndexIterator(start, end, source, TimeFilter.alwaysTrue());
        IndexIteratorBridge iter = new IndexIteratorBridge(tfIter, JexlNodeFactory.buildEQNode("FOO", "value1"), "FOO");
        iter.seek(new Range(), Collections.emptyList(), false);

        assertTrue(iter.hasNext());
        Key k = iter.next();
        assertEquals("received " + k.toStringNoTime(), "datatype\u00001", k.getColumnFamily().toString());
        assertFalse("Iterator should only return one entry but it had more", iter.hasNext());
    }

    @Test
    public void testNotFound() {
        SortedListKeyValueIterator source = getIterator();

        Key start = new Key("row", "fi\0FOO", "value\u0000datatype\u00001");
        Key end = new Key("row", "fi\0FOO", "value\u0000datatype\u00001\uffff");

        TermFrequencyIndexIterator tfIter = new TermFrequencyIndexIterator(start, end, source, TimeFilter.alwaysTrue());
        IndexIteratorBridge iter = new IndexIteratorBridge(tfIter, JexlNodeFactory.buildEQNode("FOO", "value"), "FOO");
        iter.seek(new Range(), Collections.emptyList(), false);

        assertFalse(iter.hasNext());
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidStartKey() {
        SortedListKeyValueIterator source = getIterator();

        Key start = new Key("row", "fi\0FIELD", "value\u0000datatype");
        Key end = new Key("row", "fi\0FIELD", "value\u0000datatype\u00001\uffff");

        TermFrequencyIndexIterator tfIter = new TermFrequencyIndexIterator(start, end, source, TimeFilter.alwaysTrue());
        IndexIteratorBridge iter = new IndexIteratorBridge(tfIter, JexlNodeFactory.buildEQNode("FIELD", "value"), "FIELD");
        iter.seek(new Range(), Collections.emptyList(), false);

        fail("Should have thrown an exception on seek");
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidEndKey() {
        SortedListKeyValueIterator source = getIterator();

        Key start = new Key("row", "fi\0FIELD", "value\u0000datatype\u00001");
        Key end = new Key("row", "fi\0FIELD", "value\u0000datatype");

        TermFrequencyIndexIterator tfIter = new TermFrequencyIndexIterator(start, end, source, TimeFilter.alwaysTrue());
        IndexIteratorBridge iter = new IndexIteratorBridge(tfIter, JexlNodeFactory.buildEQNode("FIELD", "value"), "FIELD");
        iter.seek(new Range(), Collections.emptyList(), false);

        fail("Should have thrown an exception on seek");
    }

    /**
     * Builds an iterator loaded with sample events
     *
     * @return a SortedMapIterator
     */
    private SortedListKeyValueIterator getIterator() {
        final Value value = new Value(new byte[0]);
        final String row = "row";
        final String cf = "fi\0FOO";
        final String tf = "tf";
        final String cqPrefix = "value";
        final String cqSuffix = "datatype\u0000";

        List<Map.Entry<Key,Value>> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) { // 'i' takes the place of the event uid
            for (int j = 0; j < 100; j++) { // 'j' increments the value
                // add TF key, cq = datatype \0 I \0 valueJ \0 FIELD
                data.add(new AbstractMap.SimpleEntry<>(new Key(row, tf, cqSuffix + (i + 1) + '\u0000' + cqPrefix + (j + 1) + "\0FOO"), value));
            }
        }
        return new SortedListKeyValueIterator(data);
    }

    private Key getFiKey(String row, String dataType, String uid, String fieldName, String fieldValue) {
        return new Key(row, "fi" + Constants.NULL + fieldName, fieldValue + Constants.NULL + dataType + Constants.NULL + uid);
    }

    private Key getTfKey(String row, String dataType, String uid, String fieldName, String fieldValue) {
        return new Key(row, "tf", dataType + Constants.NULL + uid + Constants.NULL + fieldValue + Constants.NULL + fieldName);
    }

}
