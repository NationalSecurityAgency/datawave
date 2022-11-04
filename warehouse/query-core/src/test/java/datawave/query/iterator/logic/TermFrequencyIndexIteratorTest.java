package datawave.query.iterator.logic;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.iterator.SortedListKeyValueIterator;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.TermFrequencyAggregator;
import datawave.query.predicate.EventDataQueryExpressionFilter;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.TLDEventDataFilter;
import datawave.query.tld.TLDTermFrequencyAggregator;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TermFrequencyIndexIteratorTest {
    private SortedKeyValueIterator source;
    private TypeMetadata typeMetadata;
    private TermFrequencyAggregator aggregator;
    private Set<String> fieldsToKeep;
    private EventDataQueryFilter filter;
    
    @BeforeEach
    public void setup() throws ParseException {
        List<Map.Entry<Key,Value>> baseSource = new ArrayList<>();
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("123.345.456", "FOO", "bar"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("123.345.456", "FOO", "baz"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("123.345.456.1", "FOO", "buf"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("123.345.456.1", "FOO", "buz"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("123.345.456.2", "FOO", "alf"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("123.345.456.2", "FOO", "arm"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("123.345.456.2", "FOOT", "armfoot"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("123.345.456.3", "AFOO", "alfa"), new Value()));
        baseSource.add(new AbstractMap.SimpleEntry(getTfKey("123.345.456.3", "ZFOO", "alfz"), new Value()));
        
        source = new SortedListKeyValueIterator(baseSource);
        
        String lcNoDiacritics = LcNoDiacriticsType.class.getName();
        
        typeMetadata = new TypeMetadata();
        typeMetadata.put("FOO", "type1", lcNoDiacritics);
        
        fieldsToKeep = new HashSet<>();
        fieldsToKeep.add("FOO");
        
        filter = new EventDataQueryExpressionFilter(
                        JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='buz' || FOO=='alf' || FOO=='arm'"), typeMetadata,
                        fieldsToKeep);
        aggregator = new TermFrequencyAggregator(fieldsToKeep, filter);
    }
    
    @Test
    public void testEmptyRange() throws Exception {
        Range r = new Range(getFiKey("123.345.456", "FOO", "biz"), true, getFiKey("123.345.456", "FOO", "bzz"), true);
        TermFrequencyAggregator aggregator = new TermFrequencyAggregator(null, null);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testScanMinorRange() throws Exception {
        Range r = new Range(getFiKey("123.345.456", "FOO", "baz"), true, getFiKey("123.345.456", "FOO", "baz"), true);
        TermFrequencyAggregator aggregator = new TermFrequencyAggregator(null, null);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Assertions.assertEquals("baz", (d.getDictionary().get("FOO").getData()));
    }
    
    @Test
    public void testScanMinorRangeTLD() throws Exception {
        Range r = new Range(getFiKey("123.345.456", "FOO", "baz"), true, getFiKey("123.345.456", "FOO", "baz"), true);
        TermFrequencyAggregator aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Assertions.assertEquals("baz", (d.getDictionary().get("FOO").getData()));
    }
    
    @Test
    public void testScanPartialRanges() throws Exception {
        Range r = new Range(getFiKey("123.345.456", "FOO", "alf"), false, getFiKey("123.345.456.2", "FOO", "bar"), false);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size(), d.getDictionary().size() + "");
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Assertions.assertEquals("arm", (d.getDictionary().get("FOO").getData()));
    }
    
    @Test
    public void testScanPartialRangesTLD() throws Exception {
        Range r = new Range(getFiKey("123.345.456", "FOO", "alf"), false, getFiKey("123.345.456.2", "FOO", "bar"), false);
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size(), d.getDictionary().size() + "");
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Assertions.assertEquals("arm", (d.getDictionary().get("FOO").getData()));
    }
    
    @Test
    public void testScanFullRange() throws IOException {
        Range r = new Range(getFiKey("123.345.456", "FOO", "alf"), true, getFiKey("123.345.456.2", "FOO", "buz"), true);
        
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Assertions.assertEquals(2, ((Set) d.getDictionary().get("FOO").getData()).size());
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assertions.assertEquals("bar", i.next().getValue());
        Assertions.assertEquals("baz", i.next().getValue());
        
        iterator.next();
        
        Assertions.assertTrue(iterator.hasTop());
        d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Assertions.assertEquals(2, ((Set) d.getDictionary().get("FOO").getData()).size());
        i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assertions.assertEquals("buf", i.next().getValue());
        Assertions.assertEquals("buz", i.next().getValue());
        
        iterator.next();
        
        Assertions.assertTrue(iterator.hasTop());
        d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Assertions.assertEquals(2, ((Set) d.getDictionary().get("FOO").getData()).size());
        i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assertions.assertEquals("alf", i.next().getValue());
        Assertions.assertEquals("arm", i.next().getValue());
    }
    
    @Test
    public void testScanFullRangeTLD() throws IOException {
        Range r = new Range(getFiKey("123.345.456", "FOO", "alf"), true, getFiKey("123.345.456.2", "FOO", "buz"), true);
        
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Assertions.assertEquals(6, ((Set) d.getDictionary().get("FOO").getData()).size());
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assertions.assertEquals("bar", i.next().getValue());
        Assertions.assertEquals("baz", i.next().getValue());
        Assertions.assertEquals("buf", i.next().getValue());
        Assertions.assertEquals("buz", i.next().getValue());
        Assertions.assertEquals("alf", i.next().getValue());
        Assertions.assertEquals("arm", i.next().getValue());
        
        iterator.next();
        Assertions.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testEndingFieldMismatch() throws IOException, ParseException {
        Range r = new Range(getFiKey("123.345.456.3", "FOO", "alf"), true, getFiKey("123.345.456.3", Constants.MAX_UNICODE_STRING, "buz"), false);
        filter = new EventDataQueryExpressionFilter(JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'"), typeMetadata,
                        fieldsToKeep);
        aggregator = new TermFrequencyAggregator(fieldsToKeep, filter);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        Assertions.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testScanFullRangeExclusive() throws IOException, ParseException {
        Range r = new Range(getFiKey("123.345.456", "FOO", "alf"), false, getFiKey("123.345.456.2", "FOO", "buz"), false);
        filter = new EventDataQueryExpressionFilter(JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'"), typeMetadata,
                        fieldsToKeep);
        aggregator = new TermFrequencyAggregator(fieldsToKeep, filter);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assertions.assertEquals("bar", i.next().getValue());
        Assertions.assertEquals("baz", i.next().getValue());
        
        iterator.next();
        
        Assertions.assertTrue(iterator.hasTop());
        d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assertions.assertEquals("buf", i.next().getValue());
        Assertions.assertEquals("buz", i.next().getValue());
        
        iterator.next();
        
        Assertions.assertTrue(iterator.hasTop());
        d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Assertions.assertEquals("arm", d.getDictionary().get("FOO").getData());
        
        iterator.next();
        Assertions.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testScanFullRangeExclusiveTLD() throws IOException, ParseException {
        Range r = new Range(getFiKey("123.345.456", "FOO", "alf"), false, getFiKey("123.345.456.2", "FOO", "buz"), false);
        filter = new TLDEventDataFilter(JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'"), typeMetadata, null, null, -1, -1,
                        Collections.emptyMap(), null, fieldsToKeep);
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assertions.assertEquals("bar", i.next().getValue());
        Assertions.assertEquals("baz", i.next().getValue());
        Assertions.assertEquals("buf", i.next().getValue());
        Assertions.assertEquals("arm", i.next().getValue());
        
        iterator.next();
        Assertions.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testScanFullRangeExclusiveEventDataQueryExpressionFilter() throws IOException, ParseException {
        Range r = new Range(getFiKey("123.345.456", "FOO", "alf"), false, getFiKey("123.345.456.2", "FOO", "buz"), false);
        filter = new EventDataQueryExpressionFilter(JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'"), typeMetadata,
                        fieldsToKeep);
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assertions.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assertions.assertNotNull(d);
        Assertions.assertEquals(2, d.getDictionary().size());
        Assertions.assertNotNull(d.getDictionary().get("FOO"));
        Assertions.assertNotNull(d.getDictionary().get("RECORD_ID"));
        Assertions.assertNotNull(d.getDictionary().get("FOO").getData());
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assertions.assertEquals("bar", i.next().getValue());
        Assertions.assertEquals("baz", i.next().getValue());
        Assertions.assertEquals("buf", i.next().getValue());
        Assertions.assertEquals("buz", i.next().getValue());
        Assertions.assertEquals("alf", i.next().getValue());
        Assertions.assertEquals("arm", i.next().getValue());
        
        iterator.next();
        Assertions.assertFalse(iterator.hasTop());
    }
    
    private Key getFiKey(String uid, String fieldName, String fieldValue) {
        return new Key("row", "fi" + Constants.NULL + fieldName, fieldValue + Constants.NULL + "type1" + Constants.NULL + uid);
    }
    
    private Key getTfKey(String uid, String fieldName, String fieldValue) {
        return new Key("row", "tf", "type1" + Constants.NULL + uid + Constants.NULL + fieldValue + Constants.NULL + fieldName);
    }
    
}
