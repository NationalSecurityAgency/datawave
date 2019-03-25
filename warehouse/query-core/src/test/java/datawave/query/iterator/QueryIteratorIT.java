package datawave.query.iterator;

import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static datawave.query.iterator.QueryOptions.ALLOW_FIELD_INDEX_EVALUATION;
import static datawave.query.iterator.QueryOptions.ALLOW_TERM_FREQUENCY_LOOKUP;
import static datawave.query.iterator.QueryOptions.CONTAINS_INDEX_ONLY_TERMS;
import static datawave.query.iterator.QueryOptions.END_TIME;
import static datawave.query.iterator.QueryOptions.FULL_TABLE_SCAN_ONLY;
import static datawave.query.iterator.QueryOptions.HDFS_SITE_CONFIG_URLS;
import static datawave.query.iterator.QueryOptions.INDEXED_FIELDS;
import static datawave.query.iterator.QueryOptions.INDEX_ONLY_FIELDS;
import static datawave.query.iterator.QueryOptions.IVARATOR_CACHE_BASE_URI_ALTERNATIVES;
import static datawave.query.iterator.QueryOptions.NON_INDEXED_DATATYPES;
import static datawave.query.iterator.QueryOptions.QUERY;
import static datawave.query.iterator.QueryOptions.QUERY_ID;
import static datawave.query.iterator.QueryOptions.SERIAL_EVALUATION_PIPELINE;
import static datawave.query.iterator.QueryOptions.START_TIME;
import static datawave.query.iterator.QueryOptions.TERM_FREQUENCIES_REQUIRED;
import static datawave.query.iterator.QueryOptions.TERM_FREQUENCY_FIELDS;

/**
 * Integration tests for the QueryIterator
 *
 */
public class QueryIteratorIT extends EasyMockSupport {
    protected QueryIterator iterator;
    protected SortedListKeyValueIterator baseIterator;
    protected Map<String,String> options;
    protected IteratorEnvironment environment;
    protected EventDataQueryFilter filter;
    protected TypeMetadata typeMetadata;
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    @Before
    public void setup() throws IOException {
        iterator = new QueryIterator();
        options = new HashMap<>();
        
        // global options
        
        // force serial pipelines
        options.put(SERIAL_EVALUATION_PIPELINE, "true");
        options.put(ALLOW_FIELD_INDEX_EVALUATION, "true");
        options.put(ALLOW_TERM_FREQUENCY_LOOKUP, "true");
        
        // set the indexed fields list
        options.put(INDEXED_FIELDS, "EVENT_FIELD1,EVENT_FIELD4,EVENT_FIELD6,TF_FIELD1,TF_FIELD2,INDEX_ONLY_FIELD1,INDEX_ONLY_FIELD2,INDEX_ONLY_FIELD3");
        
        // set the unindexed fields list
        options.put(NON_INDEXED_DATATYPES, "dataType1:EVENT_FIELD2,EVENT_FIELD3,EVENT_FIELD5");
        
        // set a query id
        options.put(QUERY_ID, "000001");
        
        // setup ivarator settings
        options.put(IVARATOR_CACHE_BASE_URI_ALTERNATIVES, "file://" + temporaryFolder.newFolder().getAbsolutePath());
        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        options.put(HDFS_SITE_CONFIG_URLS, hdfsSiteConfig.toExternalForm());
        
        // query time range
        options.put(START_TIME, "10");
        options.put(END_TIME, "100");
        
        // these will be marked as indexed fields
        typeMetadata = new TypeMetadata();
        typeMetadata.put("EVENT_FIELD1", "dataType1", "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("EVENT_FIELD4", "dataType1", "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("EVENT_FIELD6", "dataType1", "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("TF_FIELD1", "dataType1", "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("TF_FIELD2", "dataType1", "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("INDEX_ONLY_FIELD1", "dataType1", "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("INDEX_ONLY_FIELD2", "dataType1", "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("INDEX_ONLY_FIELD3", "dataType1", "datawave.data.type.LcNoDiacriticsType");
        
        environment = createMock(IteratorEnvironment.class);
        filter = createMock(EventDataQueryFilter.class);
    }
    
    private List<Map.Entry<Key,Value>> addEvent(long eventTime, String uid) {
        List<Map.Entry<Key,Value>> listSource = new ArrayList();
        
        // indexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("row", "EVENT_FIELD1", "a", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "EVENT_FIELD1", "a", "dataType1", uid, eventTime), new Value()));
        // unindexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("row", "EVENT_FIELD2", "b", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("row", "EVENT_FIELD3", "c", "dataType1", uid, eventTime), new Value()));
        // indexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("row", "EVENT_FIELD4", "d", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "EVENT_FIELD4", "d", "dataType1", uid, eventTime), new Value()));
        // unindexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("row", "EVENT_FIELD5", "e", "dataType1", uid, eventTime), new Value()));
        // indexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("row", "EVENT_FIELD6", "f", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "EVENT_FIELD6", "f", "dataType1", uid, eventTime), new Value()));
        
        // add some indexed TF fields
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("row", "TF_FIELD1", "a b c", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "TF_FIELD1", "a b c", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "TF_FIELD1", "a", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "TF_FIELD1", "b", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "TF_FIELD1", "c", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("row", "TF_FIELD1", "a", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("row", "TF_FIELD1", "b", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("row", "TF_FIELD1", "c", "dataType1", uid, eventTime), new Value()));
        
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent("row", "TF_FIELD2", "x y z", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "TF_FIELD2", "x y z", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "TF_FIELD2", "x", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "TF_FIELD2", "y", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "TF_FIELD2", "z", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("row", "TF_FIELD2", "x", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("row", "TF_FIELD2", "y", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF("row", "TF_FIELD2", "z", "dataType1", uid, eventTime), new Value()));
        
        // add some index only field data
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "INDEX_ONLY_FIELD1", "apple", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "INDEX_ONLY_FIELD1", "pear", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "INDEX_ONLY_FIELD1", "orange", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "INDEX_ONLY_FIELD2", "beef", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "INDEX_ONLY_FIELD2", "chicken", "dataType1", uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI("row", "INDEX_ONLY_FIELD2", "pork", "dataType1", uid, eventTime), new Value()));
        
        return listSource;
    }
    
    protected List<Map.Entry<Key,Value>> configureTestData(long eventTime) {
        List<Map.Entry<Key,Value>> listSource = new ArrayList();
        
        listSource.addAll(addEvent(eventTime, "123.345.456"));
        
        return listSource;
    }
    
    protected Range getDocumentRange(String row, String dataType, String uid) {
        // not a document range
        if (uid == null) {
            Key startKey = new Key(row);
            return new Range(startKey, true, startKey.followingKey(PartialKey.ROW), false);
        } else {
            return new Range(new Key("row", "dataType1" + Constants.NULL + uid), true, new Key("row", "dataType1" + Constants.NULL + uid + Constants.NULL),
                            false);
        }
    }
    
    @Test
    public void indexOnly_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        indexOnly_test(seekRange, "INDEX_ONLY_FIELD1 == 'apple'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        indexOnly_test(seekRange, "INDEX_ONLY_FIELD1 == 'apple'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        indexOnly_test(seekRange, "INDEX_ONLY_FIELD1 == 'fork'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        indexOnly_test(seekRange, "INDEX_ONLY_FIELD1 == 'fork'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        indexOnly_test(seekRange, "INDEX_ONLY_FIELD1 == 'apple'", false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        Map.Entry<Key,Map<String,List<String>>> secondEvent = getBaseExpectedEvent("123.345.457");
        secondEvent.getValue().put("INDEX_ONLY_FIELD1", Arrays.asList(new String[] {"apple"}));
        indexOnly_test(seekRange, "INDEX_ONLY_FIELD1 == 'apple'", false, addEvent(11, "123.345.457"), Arrays.asList(secondEvent));
    }
    
    @Test
    public void indexOnly_trailingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))", false, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))", false, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ 'f.*'))", true, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ 'f.*'))", true, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))", false, addEvent(11, "123.345.457"),
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        Map.Entry<Key,Map<String,List<String>>> secondEvent = getBaseExpectedEvent("123.345.457");
        secondEvent.getValue().put("INDEX_ONLY_FIELD1", Arrays.asList(new String[] {"apple"}));
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))", false, addEvent(11, "123.345.457"),
                        Arrays.asList(secondEvent));
    }
    
    @Test
    public void indexOnly_leadingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))", false, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))", false, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ '.*k'))", true, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ '.*k'))", true, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))", false, addEvent(11, "123.345.457"),
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        Map.Entry<Key,Map<String,List<String>>> secondEvent = getBaseExpectedEvent("123.345.457");
        secondEvent.getValue().put("INDEX_ONLY_FIELD1", Arrays.asList(new String[] {"apple"}));
        indexOnly_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))", false, addEvent(11, "123.345.457"),
                        Arrays.asList(secondEvent));
    }
    
    @Test
    public void event_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        event_test(seekRange, "EVENT_FIELD2 == 'b'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        event_test(seekRange, "EVENT_FIELD2 == 'b'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        event_test(seekRange, "EVENT_FIELD2 == 'a'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        event_test(seekRange, "EVENT_FIELD2 == 'a'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        event_test(seekRange, "EVENT_FIELD2 == 'b'", false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        event_test(seekRange, "EVENT_FIELD2 == 'b'", false, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void event_trailingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        event_test(seekRange, "EVENT_FIELD2 =~ 'b.*'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        event_test(seekRange, "EVENT_FIELD2 =~ 'b.*'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        event_test(seekRange, "EVENT_FIELD2 =~ 'a.*'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        event_test(seekRange, "EVENT_FIELD2 =~ 'a.*'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        event_test(seekRange, "EVENT_FIELD2 =~ 'b.*'", false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        event_test(seekRange, "EVENT_FIELD2 =~ 'b.*'", false, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void event_leadingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        event_test(seekRange, "EVENT_FIELD2 =~ '.*b'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        event_test(seekRange, "EVENT_FIELD2 =~ '.*b'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        event_test(seekRange, "EVENT_FIELD2 =~ '.*a'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        event_test(seekRange, "EVENT_FIELD2 =~ '.*a'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        event_test(seekRange, "EVENT_FIELD2 =~ '.*b'", false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        event_test(seekRange, "EVENT_FIELD2 =~ '.*b'", false, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void index_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        index_test(seekRange, "EVENT_FIELD4 == 'd'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        index_test(seekRange, "EVENT_FIELD4 == 'd'", false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        index_test(seekRange, "EVENT_FIELD4 == 'e'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        index_test(seekRange, "EVENT_FIELD4 == 'e'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    /**
     * Doc specific range should not find the second document
     *
     * @throws IOException
     */
    @Test
    public void index_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        index_test(seekRange, "EVENT_FIELD4 == 'd'", false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    /**
     * Shard range should find the second document
     *
     * @throws IOException
     */
    @Test
    public void index_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        index_test(seekRange, "EVENT_FIELD4 == 'd'", false, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void index_trailingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ 'd.*'))", false, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ 'd.*'))", false, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ 'e.*'))", true, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ 'e.*'))", true, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ 'd.*'))", false, addEvent(11, "123.345.457"),
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ 'd.*'))", false, addEvent(11, "123.345.457"),
                        Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void index_leadingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ '.*d'))", false, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ '.*d'))", false, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ '.*e'))", true, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ '.*e'))", true, Collections.EMPTY_LIST,
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ '.*d'))", false, addEvent(11, "123.345.457"),
                        Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        index_test(seekRange, "((ExceededValueThresholdMarkerJexlNode = true) && (EVENT_FIELD4 =~ '.*d'))", false, addEvent(11, "123.345.457"),
                        Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }

    @Test
    public void tf_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        tf_test(seekRange, "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ 'b.*'))",
                getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }

    @Test
    public void tf_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        tf_test(seekRange, "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ 'b.*'))",
                getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }

    @Test
    public void tf_exceededValue_leadingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        tf_test(seekRange, "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ '.*b'))",
                        getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("row", "dataType1", null);
        tf_test(seekRange, "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ '.*b'))",
                        getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_negated_leadingWildcard_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("row", "dataType1", "123.345.456");
        tf_test(seekRange, "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ '.*z'))",
                        getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_negated_leadingWildcard_shardRange_test() throws IOException {
        Range seekRange = getDocumentRange("row", "dataType1", null);
        tf_test(seekRange, "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ '.*z'))",
                        getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    /**
     * Simulate a full table scan against an event data (only) query
     * 
     * @param seekRange
     * @throws IOException
     */
    protected void event_test(Range seekRange, String query, boolean miss, List<Map.Entry<Key,Value>> otherData,
                    List<Map.Entry<Key,Map<String,List<String>>>> otherHits) throws IOException {
        // configure source
        List<Map.Entry<Key,Value>> listSource = configureTestData(11);
        listSource.addAll(otherData);
        
        baseIterator = new SortedListKeyValueIterator(listSource);
        
        // configure iterator
        iterator.setEvaluationFilter(filter);
        iterator.setTypeMetadata(typeMetadata);
        
        // configure specific query options
        options.put(QUERY, query);
        // none
        options.put(INDEX_ONLY_FIELDS, "");
        // set full table scan
        options.put(FULL_TABLE_SCAN_ONLY, "true");
        
        replayAll();
        
        iterator.init(baseIterator, options, environment);
        iterator.seek(seekRange, Collections.EMPTY_LIST, true);
        
        verifyAll();
        
        List<Map.Entry<Key,Map<String,List<String>>>> hits = new ArrayList<>();
        if (miss) {
            hits.add(new AbstractMap.SimpleEntry<>(null, null));
        } else {
            hits.add(getBaseExpectedEvent("123.345.456"));
        }
        hits.addAll(otherHits);
        eval(hits);
    }
    
    /**
     * Simulate an indexed query
     * 
     * @param seekRange
     * @throws IOException
     */
    protected void index_test(Range seekRange, String query, boolean miss, List<Map.Entry<Key,Value>> otherData,
                    List<Map.Entry<Key,Map<String,List<String>>>> otherHits) throws IOException {
        // configure source
        List<Map.Entry<Key,Value>> listSource = configureTestData(11);
        listSource.addAll(otherData);
        
        baseIterator = new SortedListKeyValueIterator(listSource);
        
        // configure iterator
        iterator.setEvaluationFilter(filter);
        iterator.setTypeMetadata(typeMetadata);
        
        // configure specific query options
        options.put(QUERY, query);
        // none
        options.put(INDEX_ONLY_FIELDS, "");
        
        replayAll();
        
        iterator.init(baseIterator, options, environment);
        iterator.seek(seekRange, Collections.EMPTY_LIST, true);
        
        verifyAll();
        
        List<Map.Entry<Key,Map<String,List<String>>>> hits = new ArrayList<>();
        if (miss) {
            hits.add(new AbstractMap.SimpleEntry<>(null, null));
        } else {
            hits.add(getBaseExpectedEvent("123.345.456"));
        }
        hits.addAll(otherHits);
        eval(hits);
    }
    
    /**
     * Simulate an index only query
     * 
     * @param seekRange
     * @throws IOException
     */
    protected void indexOnly_test(Range seekRange, String query, boolean miss, List<Map.Entry<Key,Value>> otherData,
                    List<Map.Entry<Key,Map<String,List<String>>>> otherHits) throws IOException {
        // configure source
        List<Map.Entry<Key,Value>> listSource = configureTestData(11);
        listSource.addAll(otherData);
        
        baseIterator = new SortedListKeyValueIterator(listSource);
        
        // configure iterator
        iterator.setEvaluationFilter(filter);
        iterator.setTypeMetadata(typeMetadata);
        
        // configure specific query options
        options.put(QUERY, query);
        // we need term frequencies
        options.put(TERM_FREQUENCIES_REQUIRED, "true");
        // set to be the index only fields required for the query?
        options.put(INDEX_ONLY_FIELDS, "INDEX_ONLY_FIELD1,INDEX_ONLY_FIELD2,INDEX_ONLY_FIELD3");
        // set because we have index only fields used
        options.put(CONTAINS_INDEX_ONLY_TERMS, "true");
        
        replayAll();
        
        iterator.init(baseIterator, options, environment);
        iterator.seek(seekRange, Collections.EMPTY_LIST, true);
        
        verifyAll();
        
        List<Map.Entry<Key,Map<String,List<String>>>> hits = new ArrayList<>();
        if (miss) {
            hits.add(new AbstractMap.SimpleEntry<>(null, null));
        } else {
            // define the base hit
            Map.Entry<Key,Map<String,List<String>>> hit = getBaseExpectedEvent("123.345.456");
            hit.getValue().put("INDEX_ONLY_FIELD1", Arrays.asList(new String[] {"apple"}));
            hits.add(hit);
        }
        
        hits.addAll(otherHits);
        eval(hits);
    }
    
    /**
     * Simulate a TF query
     *
     * @throws IOException
     */
    protected void tf_test(Range seekRange, String query, Map.Entry<Key,Map<String,List<String>>> hit, List<Map.Entry<Key,Value>> otherData,
                    List<Map.Entry<Key,Map<String,List<String>>>> otherHits) throws IOException {
        // configure source
        List<Map.Entry<Key,Value>> listSource = configureTestData(11);
        listSource.addAll(otherData);
        
        baseIterator = new SortedListKeyValueIterator(listSource);
        
        // configure iterator
        iterator.setEvaluationFilter(filter);
        iterator.setTypeMetadata(typeMetadata);
        
        // configure specific query options
        options.put(QUERY, query);
        // we need term frequencies
        options.put(TERM_FREQUENCIES_REQUIRED, "true");
        // set to be the index only fields required for the query?
        options.put(INDEX_ONLY_FIELDS, "INDEX_ONLY_FIELD1,INDEX_ONLY_FIELD2,INDEX_ONLY_FIELD3");
        // set t obe the term frequency fields required for the query?
        options.put(TERM_FREQUENCY_FIELDS, "TF_FIELD1,TF_FIELD2");
        
        replayAll();
        
        iterator.init(baseIterator, options, environment);
        iterator.seek(seekRange, Collections.EMPTY_LIST, true);
        
        verifyAll();
        
        List<Map.Entry<Key,Map<String,List<String>>>> hits = new ArrayList<>();
        if (hit != null) {
            hits.add(hit);
        }
        
        hits.addAll(otherHits);
        eval(hits);
    }
    
    protected Map.Entry<Key,Map<String,List<String>>> getBaseExpectedEvent(String uid) {
        Key hitKey = new Key("row", "dataType1" + Constants.NULL + uid);
        Map<String,List<String>> expectedDocument = new HashMap<>();
        expectedDocument.put("EVENT_FIELD1", Arrays.asList(new String[] {"a"}));
        expectedDocument.put("EVENT_FIELD2", Arrays.asList(new String[] {"b"}));
        expectedDocument.put("EVENT_FIELD3", Arrays.asList(new String[] {"c"}));
        expectedDocument.put("EVENT_FIELD4", Arrays.asList(new String[] {"d"}));
        expectedDocument.put("EVENT_FIELD5", Arrays.asList(new String[] {"e"}));
        expectedDocument.put("EVENT_FIELD6", Arrays.asList(new String[] {"f"}));
        
        expectedDocument.put("TF_FIELD1", Arrays.asList(new String[] {"a b c"}));
        expectedDocument.put("TF_FIELD2", Arrays.asList(new String[] {"x y z"}));
        
        return new AbstractMap.SimpleEntry<>(hitKey, expectedDocument);
    }
    
    protected void eval(List<Map.Entry<Key,Map<String,List<String>>>> toEval) throws IOException {
        Iterator<Map.Entry<Key,Map<String,List<String>>>> evalIterator = toEval.iterator();
        while (evalIterator.hasNext()) {
            Map.Entry<Key,Map<String,List<String>>> evalPair = evalIterator.next();
            eval(evalPair.getKey(), evalPair.getValue());
        }
        Assert.assertFalse(iterator.hasTop());
    }
    
    /**
     * Deserialize and evaluate the document, expects 0 to 1 documents
     *
     * @param docKeyHit
     *            the expected hit Key, null if no hit expected
     * @param docKeys
     *            the expected values
     * @throws IOException
     */
    protected void eval(Key docKeyHit, Map<String,List<String>> docKeys) throws IOException {
        // asserts for a hit or miss
        if (docKeyHit == null) {
            Assert.assertFalse(iterator.hasTop());
        } else {
            Assert.assertTrue("Expected hit, but got none", iterator.hasTop());
            Key next = iterator.getTopKey();
            Assert.assertTrue(next != null);
            Assert.assertTrue(next.getRow().toString().equals(docKeyHit.getRow().toString()));
            Assert.assertTrue(next.getColumnFamily().toString().equals(docKeyHit.getColumnFamily().toString()));
            
            // asserts for document build
            Value topValue = iterator.getTopValue();
            Assert.assertTrue(topValue != null);
            Map.Entry<Key,Document> deserializedValue = deserialize(topValue);
            Assert.assertTrue(deserializedValue.getValue() != null);
            Document d = deserializedValue.getValue();
            Assert.assertTrue(d != null);
            
            // +1 is for RECORD_ID field
            Assert.assertTrue("Unexpected doc size: " + d.getDictionary().size() + "\nGot: " + d.getDictionary() + "\n" + "expected: " + docKeys, docKeys
                            .keySet().size() + 1 == d.getDictionary().size());
            
            // verify hits for each specified field
            for (String field : docKeys.keySet()) {
                List<String> expected = docKeys.get(field);
                if (expected.size() == 1) {
                    // verify the only doc
                    Assert.assertTrue("value: " + d.getDictionary().get(field).getData() + " did not match expected value: " + expected.get(0), d
                                    .getDictionary().get(field).getData().toString().equals(expected.get(0)));
                } else {
                    // the data should be a set, verify it matches expected
                    Object dictData = d.getDictionary().get(field).getData();
                    Assert.assertTrue(dictData != null);
                    Assert.assertTrue("Expected " + expected.size() + " values for '" + field + "' found 1, '" + dictData.toString() + "'\nexpected: "
                                    + expected, dictData instanceof Set);
                    Set dictSet = (Set) dictData;
                    Assert.assertTrue("Expected " + expected.size() + " values for '" + field + "' found " + dictSet.size() + "\nfound: " + dictSet.toString()
                                    + "\nexpected: " + expected, dictSet.size() == expected.size());
                    Iterator<Attribute> dictIterator = dictSet.iterator();
                    while (dictIterator.hasNext()) {
                        Assert.assertTrue(expected.remove(dictIterator.next().getData().toString()));
                    }
                    // verify that the expected set is now empty
                    Assert.assertTrue(expected.size() == 0);
                }
            }
            
            // there should be no other hits
            iterator.next();
        }
    }
    
    private Map.Entry<Key,Document> deserialize(Value value) {
        KryoDocumentDeserializer dser = new KryoDocumentDeserializer();
        return dser.apply(new AbstractMap.SimpleEntry(null, value));
    }
    
    // support methods
    protected Key getTF(String row, String field, String value, String dataType, String uid, long timestamp) {
        // CQ = dataType\0UID\0Normalized field value\0Field name
        return new Key(row, "tf", dataType + Constants.NULL_BYTE_STRING + uid + Constants.NULL_BYTE_STRING + value + Constants.NULL_BYTE_STRING + field,
                        timestamp);
    }
    
    protected Key getFI(String row, String field, String value, String dataType, String uid, long timestamp) {
        return new Key(row, "fi" + Constants.NULL_BYTE_STRING + field.toUpperCase(), value + Constants.NULL_BYTE_STRING + dataType + Constants.NULL_BYTE_STRING
                        + uid, timestamp);
    }
    
    protected Key getEvent(String row, String field, String value, String dataType, String uid, long timestamp) {
        return new Key(row, dataType + Constants.NULL + uid, field + Constants.NULL + value, timestamp);
    }
}
