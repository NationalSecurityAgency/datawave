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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        
        fieldsToKeep = new HashSet<>();
        fieldsToKeep.add("FOO");
        
        filter = new EventDataQueryExpressionFilter(
                        JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='buz' || FOO=='alf' || FOO=='arm'"), typeMetadata,
                        fieldsToKeep);
        aggregator = new TermFrequencyAggregator(fieldsToKeep, filter);
    }
    
    @Test
    public void testEmptyRange() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "biz"), true, getFiKey("row", "type1", "123.345.456", "FOO", "bzz"), true);
        TermFrequencyAggregator aggregator = new TermFrequencyAggregator(null, null);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testScanMinorRange() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "baz"), true, getFiKey("row", "type1", "123.345.456", "FOO", "baz"), true);
        TermFrequencyAggregator aggregator = new TermFrequencyAggregator(null, null);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Assert.assertTrue((d.getDictionary().get("FOO").getData()).equals("baz"));
    }
    
    @Test
    public void testScanMinorRangeTLD() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "baz"), true, getFiKey("row", "type1", "123.345.456", "FOO", "baz"), true);
        TermFrequencyAggregator aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Assert.assertTrue((d.getDictionary().get("FOO").getData()).equals("baz"));
    }
    
    @Test
    public void testScanPartialRanges() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "bar"), false);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() + "", d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Assert.assertTrue((d.getDictionary().get("FOO").getData()).equals("arm"));
    }
    
    @Test
    public void testScanPartialRangesTLD() throws Exception {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "bar"), false);
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() + "", d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Assert.assertTrue((d.getDictionary().get("FOO").getData()).equals("arm"));
    }
    
    @Test
    public void testScanFullRange() throws IOException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), true, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), true);
        
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Assert.assertTrue(((Set) d.getDictionary().get("FOO").getData()).size() == 2);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assert.assertTrue(i.next().getValue().equals("bar"));
        Assert.assertTrue(i.next().getValue().equals("baz"));
        
        iterator.next();
        
        Assert.assertTrue(iterator.hasTop());
        d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Assert.assertTrue(((Set) d.getDictionary().get("FOO").getData()).size() == 2);
        i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assert.assertTrue(i.next().getValue().equals("buf"));
        Assert.assertTrue(i.next().getValue().equals("buz"));
        
        iterator.next();
        
        Assert.assertTrue(iterator.hasTop());
        d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Assert.assertTrue(((Set) d.getDictionary().get("FOO").getData()).size() == 2);
        i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assert.assertTrue(i.next().getValue().equals("alf"));
        Assert.assertTrue(i.next().getValue().equals("arm"));
    }
    
    @Test
    public void testScanFullRangeTLD() throws IOException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), true, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), true);
        
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Assert.assertTrue(((Set) d.getDictionary().get("FOO").getData()).size() == 6);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assert.assertTrue(i.next().getValue().equals("bar"));
        Assert.assertTrue(i.next().getValue().equals("baz"));
        Assert.assertTrue(i.next().getValue().equals("buf"));
        Assert.assertTrue(i.next().getValue().equals("buz"));
        Assert.assertTrue(i.next().getValue().equals("alf"));
        Assert.assertTrue(i.next().getValue().equals("arm"));
        
        iterator.next();
        Assert.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testEndingFieldMismatch() throws IOException, ParseException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456.3", "FOO", "alf"), true, getFiKey("row", "type1", "123.345.456.3",
                        Constants.MAX_UNICODE_STRING, "buz"), false);
        filter = new EventDataQueryExpressionFilter(JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'"), typeMetadata,
                        fieldsToKeep);
        aggregator = new TermFrequencyAggregator(fieldsToKeep, filter);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        Assert.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testScanFullRangeExclusive() throws IOException, ParseException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), false);
        filter = new EventDataQueryExpressionFilter(JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'"), typeMetadata,
                        fieldsToKeep);
        aggregator = new TermFrequencyAggregator(fieldsToKeep, filter);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assert.assertTrue(i.next().getValue().equals("bar"));
        Assert.assertTrue(i.next().getValue().equals("baz"));
        
        iterator.next();
        
        Assert.assertTrue(iterator.hasTop());
        d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assert.assertTrue(i.next().getValue().equals("buf"));
        Assert.assertTrue(i.next().getValue().equals("buz"));
        
        iterator.next();
        
        Assert.assertTrue(iterator.hasTop());
        d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData().equals("arm"));
        
        iterator.next();
        Assert.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testScanFullRangeExclusiveTLD() throws IOException, ParseException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), false);
        filter = new TLDEventDataFilter(JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'"), typeMetadata, null, null, -1, -1,
                        Collections.emptyMap(), null, fieldsToKeep);
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assert.assertTrue(i.next().getValue().equals("bar"));
        Assert.assertTrue(i.next().getValue().equals("baz"));
        Assert.assertTrue(i.next().getValue().equals("buf"));
        Assert.assertTrue(i.next().getValue().equals("arm"));
        
        iterator.next();
        Assert.assertFalse(iterator.hasTop());
    }
    
    @Test
    public void testScanFullRangeExclusiveEventDataQueryExpressionFilter() throws IOException, ParseException {
        Range r = new Range(getFiKey("row", "type1", "123.345.456", "FOO", "alf"), false, getFiKey("row", "type1", "123.345.456.2", "FOO", "buz"), false);
        filter = new EventDataQueryExpressionFilter(JexlASTHelper.parseJexlQuery("FOO=='bar' || FOO=='baz' || FOO=='buf' || FOO=='arm'"), typeMetadata,
                        fieldsToKeep);
        aggregator = new TLDTermFrequencyAggregator(fieldsToKeep, filter, -1);
        TermFrequencyIndexIterator iterator = new TermFrequencyIndexIterator(r, source, null, typeMetadata, true, null, aggregator);
        
        // jump to the first doc
        iterator.seek(null, null, true);
        
        Assert.assertTrue(iterator.hasTop());
        Document d = iterator.document();
        Assert.assertTrue(d != null);
        Assert.assertTrue(d.getDictionary().size() == 2);
        Assert.assertTrue(d.getDictionary().get("FOO") != null);
        Assert.assertTrue(d.getDictionary().get("RECORD_ID") != null);
        Assert.assertTrue(d.getDictionary().get("FOO").getData() != null);
        Iterator<PreNormalizedAttribute> i = ((Set) d.getDictionary().get("FOO").getData()).iterator();
        Assert.assertTrue(i.next().getValue().equals("bar"));
        Assert.assertTrue(i.next().getValue().equals("baz"));
        Assert.assertTrue(i.next().getValue().equals("buf"));
        Assert.assertTrue(i.next().getValue().equals("buz"));
        Assert.assertTrue(i.next().getValue().equals("alf"));
        Assert.assertTrue(i.next().getValue().equals("arm"));
        
        iterator.next();
        Assert.assertFalse(iterator.hasTop());
    }
    
    private Key getFiKey(String row, String dataType, String uid, String fieldName, String fieldValue) {
        return new Key(row, "fi" + Constants.NULL + fieldName, fieldValue + Constants.NULL + dataType + Constants.NULL + uid);
    }
    
    private Key getTfKey(String row, String dataType, String uid, String fieldName, String fieldValue) {
        return new Key(row, "tf", dataType + Constants.NULL + uid + Constants.NULL + fieldValue + Constants.NULL + fieldName);
    }
    
}
