package datawave.query.iterator;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import datawave.core.iterators.filesystem.FileSystemCache;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.IteratorBuildingVisitor;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.TypeMetadata;
import datawave.util.StringUtils;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static datawave.query.iterator.QueryOptions.*;
import static org.apache.commons.pool.impl.GenericObjectPool.WHEN_EXHAUSTED_BLOCK;

/**
 * Integration tests for the IteratorBuildingVisitor
 *
 */
public class IteratorBuildingVisitorIT extends EasyMockSupport implements SourceFactory<Key,Value> {
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    protected IteratorBuildingVisitor iterator;
    protected SortedListKeyValueIterator baseIterator;
    protected Map<String,String> options;
    protected IteratorEnvironment environment;
    protected EventDataQueryFilter filter;
    protected TypeMetadata typeMetadata;
    protected FileSystemCache  fsCache;
    // Default row is for day 20190314, shard 0
    protected static final String DEFAULT_ROW = "20190314_0";
    
    // Default if test does not specify a datatype
    protected static final String DEFAULT_DATATYPE = "dataType1";
    
    public Path tempPath;

    public static Set<String> buildFieldSetFromString(String fieldStr) {
        Set<String> fields = new HashSet<>();
        for (String field : StringUtils.split(fieldStr, ',')) {
            if (!org.apache.commons.lang.StringUtils.isBlank(field)) {
                fields.add(field);
            }
        }
        return fields;
    }

    @Before
    public void setup() throws IOException {
        //iterator = new QueryIterator();
        iterator = new IteratorBuildingVisitor();
        iterator.setAllowTermFrequencyLookup(true);
        options = new HashMap<>();
        tempPath = temporaryFolder.newFolder().toPath();
        
        // global options
        
        // force serial pipelines
        options.put(SERIAL_EVALUATION_PIPELINE, "true");
        options.put(ALLOW_FIELD_INDEX_EVALUATION, "true");
        options.put(ALLOW_TERM_FREQUENCY_LOOKUP, "true");
        
        // set the indexed fields list

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(new ByteArrayOutputStream());
        oos.writeObject(buildFieldSetFromString("EVENT_FIELD1,EVENT_FIELD4,EVENT_FIELD6,TF_FIELD0,TF_FIELD1,TF_FIELD2,INDEX_ONLY_FIELD1,INDEX_ONLY_FIELD2,INDEX_ONLY_FIELD3"));
        options.put(INDEXED_FIELDS,
                "EVENT_FIELD1,EVENT_FIELD4,EVENT_FIELD6,TF_FIELD0,TF_FIELD1,TF_FIELD2,INDEX_ONLY_FIELD1,INDEX_ONLY_FIELD2,INDEX_ONLY_FIELD3");
        
        // set the unindexed fields list
        options.put(NON_INDEXED_DATATYPES, DEFAULT_DATATYPE + ":EVENT_FIELD2,EVENT_FIELD3,EVENT_FIELD5");
        iterator.setTermFrequencyFields(buildFieldSetFromString("TF_FIELD0,TF_FIELD1,TF_FIELD2"));
        // set a query id
        options.put(QUERY_ID, "000001");
        
        // setup ivarator settings
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig("file://" + tempPath.toAbsolutePath().toString());
        options.put(IVARATOR_CACHE_DIR_CONFIG, IvaratorCacheDirConfig.toJson(config));
        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        options.put(HDFS_SITE_CONFIG_URLS, hdfsSiteConfig.toExternalForm());
        fsCache = new FileSystemCache(hdfsSiteConfig.toExternalForm());
        
        // query time range
        options.put(START_TIME, "10");
        options.put(END_TIME, "100");
        
        // these will be marked as indexed fields
        typeMetadata = new TypeMetadata();
        typeMetadata.put("EVENT_FIELD1", DEFAULT_DATATYPE, "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("EVENT_FIELD4", DEFAULT_DATATYPE, "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("EVENT_FIELD6", DEFAULT_DATATYPE, "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("TF_FIELD0", DEFAULT_DATATYPE, "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("TF_FIELD1", DEFAULT_DATATYPE, CommaFreeType.class.getName());
        typeMetadata.put("TF_FIELD2", DEFAULT_DATATYPE, CommaFreeType.class.getName());
        typeMetadata.put("INDEX_ONLY_FIELD1", DEFAULT_DATATYPE, "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("INDEX_ONLY_FIELD2", DEFAULT_DATATYPE, "datawave.data.type.LcNoDiacriticsType");
        typeMetadata.put("INDEX_ONLY_FIELD3", DEFAULT_DATATYPE, "datawave.data.type.LcNoDiacriticsType");
        
        environment = createMock(IteratorEnvironment.class);
        EasyMock.expect(environment.getConfig()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
        filter = createMock(EventDataQueryFilter.class);
    }
    
    @After
    public void cleanUp() {
        tempPath.toFile().deleteOnExit();
    }
    
    private List<Map.Entry<Key,Value>> addEvent(long eventTime, String uid) {
        return addEvent(DEFAULT_ROW, DEFAULT_DATATYPE, eventTime, uid);
    }
    
    private List<Map.Entry<Key,Value>> addEvent(String row, String dataType, long eventTime, String uid) {
        List<Map.Entry<Key,Value>> listSource = new ArrayList<>();
        
        // indexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD1", "a", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "EVENT_FIELD1", "a", dataType, uid, eventTime), new Value()));
        // unindexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD2", "b", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD3", "c", dataType, uid, eventTime), new Value()));
        // indexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD4", "d", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "EVENT_FIELD4", "d", dataType, uid, eventTime), new Value()));
        // unindexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD5", "e", dataType, uid, eventTime), new Value()));
        // indexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD6", "f", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "EVENT_FIELD6", "f", dataType, uid, eventTime), new Value()));
        
        // add some indexed TF fields
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(DEFAULT_ROW, "TF_FIELD1", "a,, b,,, c,,", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD1", "a b c", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD1", "a", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD1", "b", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD1", "c", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD1", "a", dataType, uid, eventTime), getTFValue(0)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD1", "b", dataType, uid, eventTime), getTFValue(1)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD1", "c", dataType, uid, eventTime), getTFValue(2)));
        
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "TF_FIELD2", ",x, ,y, ,z,", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD2", "x y z", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD2", "x", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD2", "y", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD2", "z", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD2", "x", dataType, uid, eventTime), getTFValue(23)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD2", "y", dataType, uid, eventTime), getTFValue(24)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD2", "z", dataType, uid, eventTime), getTFValue(25)));
        
        // add an index only TF
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD4", "d", dataType, uid, eventTime), getTFValue(3)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD4", "d", dataType, uid, eventTime), getTFValue(3)));
        
        // add some index only field data
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD1", "apple", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD1", "pear", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD1", "orange", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD2", "beef", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD2", "chicken", dataType, uid, eventTime), new Value()));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD2", "pork", dataType, uid, eventTime), new Value()));
        
        return listSource;
    }
    
    protected List<Map.Entry<Key,Value>> configureTestData(long eventTime) {
        List<Map.Entry<Key,Value>> listSource = new ArrayList<>();
        
        listSource.addAll(addEvent(eventTime, "123.345.456"));
        
        return listSource;
    }
    
    protected Range getDocumentRange(String uid) {
        return getDocumentRange(DEFAULT_ROW, DEFAULT_DATATYPE, uid);
    }
    
    protected Range getDocumentRange(String row, String dataType, String uid) {
        // Check for a shard range
        if (uid == null) {
            return getShardRange(row);
        }
        
        Key startKey = new Key(row, dataType + Constants.NULL + uid);
        Key endKey = new Key(row, dataType + Constants.NULL + uid + Constants.NULL);
        return new Range(startKey, true, endKey, false);
    }
    
    protected Range getShardRange() {
        return getShardRange(DEFAULT_ROW);
    }
    
    protected Range getShardRange(String row) {
        Key startKey = new Key(row);
        return new Range(startKey, true, startKey.followingKey(PartialKey.ROW), false);
    }
    
    @Test
    public void indexOnly_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_documentSpecific_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_shardRange_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        indexOnly_test(seekRange, "INDEX_ONLY_FIELD1 == 'fork'", true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "INDEX_ONLY_FIELD1 == 'fork'";
        indexOnly_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);

    }
    
    @Test
    public void indexOnly_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull\
        //for (int i=0; i < 20000; i++) {
            Range seekRange = getShardRange();
            String query = "INDEX_ONLY_FIELD1 == 'apple'";
            Map.Entry<Key, Map<String, List<String>>> secondEvent = getBaseExpectedEvent("123.345.457");
            secondEvent.getValue().put("INDEX_ONLY_FIELD1", Arrays.asList(new String[]{"apple"}));
            indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Arrays.asList(secondEvent));
        //}
    }
    
    @Test
    public void indexOnly_trailingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'f.*'))";
        indexOnly_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'f.*'))";
        indexOnly_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))";
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_trailingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))";
        Map.Entry<Key,Map<String,List<String>>> secondEvent = getBaseExpectedEvent("123.345.457");
        secondEvent.getValue().put("INDEX_ONLY_FIELD1", Arrays.asList(new String[] {"apple"}));
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Arrays.asList(secondEvent));
    }
    
    @Test
    public void indexOnly_leadingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*k'))";
        indexOnly_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*k'))";
        indexOnly_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))";
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_leadingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))";
        Map.Entry<Key,Map<String,List<String>>> secondEvent = getBaseExpectedEvent("123.345.457");
        secondEvent.getValue().put("INDEX_ONLY_FIELD1", Arrays.asList(new String[] {"apple"}));
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Arrays.asList(secondEvent));
    }
    
    @Test
    public void event_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_documentSpecific_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_shardRange_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 == 'a'";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 == 'a'";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void event_trailingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ 'b.*'";
        event_test(seekRange, query, false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ 'b.*'";
        event_test(seekRange, query, false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ 'a.*'";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ 'a.*'";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ 'b.*'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_trailingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ 'b.*'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void event_leadingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ '.*b'";
        event_test(seekRange, query, false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ '.*b'";
        event_test(seekRange, query, false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ '.*a'";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ '.*a'";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ '.*b'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_leadingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ '.*b'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }

    @Test
    public void index_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_documentSpecific_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_shardRange_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD4 == 'e'";
        index_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD4 == 'e'";
        index_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    /**
     * Doc specific range should not find the second document
     *
     * @throws IOException
     */
    @Test
    public void index_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    /**
     * Shard range should find the second document
     *
     * @throws IOException
     */
    @Test
    public void index_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void index_trailingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'd.*'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'd.*'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'e.*'))";
        index_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'e.*'))";
        index_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'd.*'))";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_trailingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'd.*'))";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void index_leadingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*d'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "true && ((_Value_ = true) && (EVENT_FIELD4 =~ '.*d'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "true && ((_Value_ = true) && (EVENT_FIELD4 =~ '.*e'))";
        index_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "true && ((_Value_ = true) && (EVENT_FIELD4 =~ '.*e'))";
        index_test(seekRange, query, true, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*d'))";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void index_leadingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*d'))";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Arrays.asList(getBaseExpectedEvent("123.345.457")));
    }
    
    @Test
    public void tf_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ 'b.*'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_negation_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 !~ 'b.*'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_index_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 !~ 'y.*'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_event_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ 'b.*'))";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ 'b.*'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_negation_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 !~ 'b.*'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_index_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 !~ 'z.*'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_event_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ 'b.*'))";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_leadingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_negation_exceededValue_leadingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 !~ '.*b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_index_exceededValue_leadingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 !~ '.*x'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_negation_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 !~ '.*b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_index_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 !~ '.*a'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_event_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ '.*b'))";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_negated_leadingWildcard_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_negation_exceededValue_negated_leadingWildcard_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 !~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_index_exceededValue_negated_leadingWildcard_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 !~ '.*z'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_event_exceededValue_negated_leadingWildcard_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 =~ '.*z'))";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_negated_leadingWildcard_multiTerm_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ '.*b c'))";
        tf_test(seekRange, query, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_negated_leadingWildcard_multiTerm_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD1 =~ '.*b c'))";
        tf_test(seekRange, query, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        
    }
    
    @Test
    public void tf_exceededValue_negated_leadingWildcard_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_event_exceededValue_negated_leadingWildcard_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 =~ '.*z'))";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_negated_leadingWildcardMissIndexOnly_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD4 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_negation_exceededValue_negated_leadingWildcardMissIndexOnly_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 !~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_index_exceededValue_negated_leadingWildcardMissIndexOnly_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 !~ '.*z'))";
        index_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_event_exceededValue_negated_leadingWildcardMissIndexOnly_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 =~ '.*z'))";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_exceededValue_negated_leadingWildcardMissIndexOnly_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD4 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    // terms 'a' and 'b' are adjacent, thus a valid phrase
    @Test
    public void tf_contentFunction_validPhrase_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((TF_FIELD1 =='a' && TF_FIELD1 =='b') && content:phrase(TF_FIELD1,termOffsetMap,'a','b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    // terms 'a' and 'c' do not appear adjacent
    @Test
    public void tf_contentFunction_invalidPhrase_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((TF_FIELD1 =='a' && TF_FIELD1 =='c') && content:phrase(TF_FIELD1,termOffsetMap,'a','c'))";
        tf_test(seekRange, query, new AbstractMap.SimpleEntry(null, null), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    // terms 'a' and 'b' are adjacent, thus a valid phrase
    @Test
    public void tf_contentFunction_validPhrase_docRange_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((TF_FIELD1 =='a' && TF_FIELD1 =='b') && content:phrase(TF_FIELD1,termOffsetMap,'a','b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    // terms 'a' and 'c' do not appear adjacent
    @Test
    public void tf_contentFunction_invalidPhrase_docRange_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((TF_FIELD1 =='a' && TF_FIELD1 =='c') && content:phrase(TF_FIELD1,termOffsetMap,'a','c'))";
        tf_test(seekRange, query, new AbstractMap.SimpleEntry(null, null), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    // A && (phrase:'a,b' || phrase:'j,k')
    // Second phrase for TF_FIELD0 that does not appear in the document should be pruned from the TF FieldValues multimap.
    @Test
    public void tf_contentFunction_validPhrase_docRange_test2() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && (((TF_FIELD1 =='a' && TF_FIELD1 =='b') && content:phrase(TF_FIELD1,termOffsetMap,'a','b')) || "
                        + "((TF_FIELD0 =='j' && TF_FIELD0 =='k') && content:phrase(TF_FIELD0,termOffsetMap,'j','k')))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    // A && (phrase:'a,b' || phrase:'j,k')
    // Second phrase for TF_FIELD0 that does not appear in the document should be pruned from the TF FieldValues multimap.
    @Test
    public void tf_contentFunction_validPhrase_shardRange_test3() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && (((TF_FIELD1 =='a' && TF_FIELD1 =='b') && content:phrase(TF_FIELD1,termOffsetMap,'a','b')) || "
                        + "((TF_FIELD0 =='j' && TF_FIELD0 =='k') && content:phrase(TF_FIELD0,termOffsetMap,'j','k')))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void tf_event_exceededValue_negated_leadingWildcardMissIndexOnly_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((ExceededValueThresholdMarkerJexlNode = true) && (TF_FIELD2 =~ '.*z'))";
        event_test(seekRange, query, true, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    protected void configureIterator() {
        // configure iterator
        iterator.setTypeMetadata(typeMetadata);
    }
    
    /**
     * Simulate a full table scan against an event data (only) query
     *
     * @param seekRange
     * @throws IOException
     */
    protected void event_test(Range seekRange, String query, boolean miss, Map.Entry<Key,Map<String,List<String>>> hitOverride,
                    List<Map.Entry<Key,Value>> otherData, List<Map.Entry<Key,Map<String,List<String>>>> otherHits) throws IOException {
        // configure source
        List<Map.Entry<Key,Value>> listSource = configureTestData(11);
        listSource.addAll(otherData);
        
        baseIterator = new SortedListKeyValueIterator(listSource);
        
        configureIterator();
        
        // configure specific query options
        options.put(QUERY, query);
        // none
        options.put(INDEX_ONLY_FIELDS, "");
        // set full table scan
        options.put(FULL_TABLE_SCAN_ONLY, "true");
        
        replayAll();


//        iterator.ini
        iterator.setSource(baseIterator,environment);
        iterator.limit(seekRange);
        //iterator.init(baseIterator, options, environment);
        //iterator.seek(seekRange, Collections.EMPTY_LIST, true);
        
        verifyAll();
        
        List<Map.Entry<Key,Map<String,List<String>>>> hits = new ArrayList<>();
        if (miss) {
            hits.add(new AbstractMap.SimpleEntry<>(null, null));
        } else {
            if (hitOverride != null) {
                hits.add(hitOverride);
            } else {
                hits.add(getBaseExpectedEvent("123.345.456"));
            }
        }
        hits.addAll(otherHits);
        eval(seekRange,query, hits);
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
        
        configureIterator();
        
        // configure specific query options
        options.put(QUERY, query);
        // none
        options.put(INDEX_ONLY_FIELDS, "");
        
        replayAll();
        iterator.setSource(baseIterator,environment);
        iterator.limit(seekRange);


        iterator.setSource(baseIterator,environment);
        iterator.setUnsortedIvaratorSource(baseIterator.deepCopy(environment));
        //iterator.limit(seekRange);
        iterator.setTimeFilter(new TimeFilter(0,System.currentTimeMillis()));
        iterator.setQueryId(UUID.randomUUID().toString());
        iterator.setScanId("1");
        iterator.setHdfsFileSystem(fsCache);
        iterator.setIvaratorSourcePool(createIvaratorSourcePool(5));
        iterator.setIvaratorCacheDirConfigs(IvaratorCacheDirConfig.fromJson(options.get(IVARATOR_CACHE_DIR_CONFIG)));

        verifyAll();
        
        List<Map.Entry<Key,Map<String,List<String>>>> hits = new ArrayList<>();
        if (miss) {
            hits.add(new AbstractMap.SimpleEntry<>(null, null));
        } else {
            hits.add(getBaseExpectedEvent("123.345.456"));
        }
        hits.addAll(otherHits);
        eval(seekRange,query,hits);
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
        
        configureIterator();
        
        // configure specific query options
        options.put(QUERY, query);
        // we need term frequencies
        options.put(FILTER_MASKED_VALUES, "false");
        options.put(Constants.RETURN_TYPE, "json");
        options.put(TERM_FREQUENCIES_REQUIRED, "true");
        // set to be the index only fields required for the query?
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(new ByteArrayOutputStream());

        oos.writeObject(QueryOptions.buildFieldSetStringFromSet(buildFieldSetFromString("INDEX_ONLY_FIELD1,INDEX_ONLY_FIELD2,INDEX_ONLY_FIELD3,TF_FIELD4")));
        options.put(INDEX_ONLY_FIELDS, out.toString());
        // set because we have index only fields used
        options.put(CONTAINS_INDEX_ONLY_TERMS, "true");
        options.put(TRACK_SIZES, "false");
        
        replayAll();

        iterator.setSource(baseIterator,environment);
        iterator.limit(seekRange);
        //iterator.init(baseIterator, options, environment);
        //iterator.seek(seekRange, Collections.EMPTY_LIST, true);

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
        //eval(hits);
    }
    
    protected void tf_test(Range seekRange, String query, Map.Entry<Key,Map<String,List<String>>> hit, List<Map.Entry<Key,Value>> otherData,
                    List<Map.Entry<Key,Map<String,List<String>>>> otherHits) throws IOException {
        // configure source
        List<Map.Entry<Key,Value>> listSource = configureTestData(11);
        listSource.addAll(otherData);
        
        baseIterator = new SortedListKeyValueIterator(listSource);
        
        configureIterator();
        
        // configure specific query options
        options.put(QUERY, query);
        // we need term frequencies
        options.put(TERM_FREQUENCIES_REQUIRED, "true");
        // set to be the index only fields required for the query?
        options.put(INDEX_ONLY_FIELDS, "INDEX_ONLY_FIELD1,INDEX_ONLY_FIELD2,INDEX_ONLY_FIELD3,TF_FIELD4");
        // set to be the term frequency fields required for the query?
        options.put(TERM_FREQUENCY_FIELDS, "TF_FIELD0,TF_FIELD1,TF_FIELD2,TF_FIELD4");
        
        replayAll();

        Set<String> indexOnly = Sets.newHashSet(Splitter.on(",").split(options.get(INDEX_ONLY_FIELDS)));
        Set<String> tfFields = Sets.newHashSet(Splitter.on(",").split(options.get(TERM_FREQUENCY_FIELDS)));

        iterator.setIndexOnlyFields(indexOnly);
        iterator.setTermFrequencyFields(tfFields);
        IvaratorCacheDirConfig cacheDir = new IvaratorCacheDirConfig("file:///" + tempPath.toFile().getCanonicalPath());
        iterator.setIvaratorCacheDirConfigs(Lists.newArrayList(cacheDir));

        iterator.setSource(baseIterator,environment);
        iterator.setUnsortedIvaratorSource(baseIterator.deepCopy(environment));
        //iterator.limit(seekRange);
        iterator.setTimeFilter(new TimeFilter(0,System.currentTimeMillis()));
        iterator.setQueryId(UUID.randomUUID().toString());
        iterator.setScanId("1");
        iterator.setHdfsFileSystem(fsCache);
        iterator.setIvaratorSourcePool(createIvaratorSourcePool(5));


        /*try {
            var rangeScript = JexlASTHelper.parseJexlQuery(query);

            rangeScript.jjtAccept(iterator, null);

            var sourceIter = iterator.root();
            sourceIter.initialize();
            sourceIter.setEnvironment(environment);
            var seekableIter = new SeekableNestedIterator(sourceIter, environment);

            seekableIter.seek(seekRange,Collections.EMPTY_LIST, true);
            while(seekableIter.hasNext()){
                System.out.println(seekableIter.next());
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }*/

        

        //iterator.init(baseIterator, options, environment);
        //iterator.seek(seekRange, Collections.EMPTY_LIST, true);
        
        verifyAll();
        
        List<Map.Entry<Key,Map<String,List<String>>>> hits = new ArrayList<>();
        if (hit != null) {
            hits.add(hit);
        }
        
        hits.addAll(otherHits);
        eval(seekRange,query, hits);
    }
    
    @Test
    public void event_isNotNull_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        event_test(seekRange, "EVENT_FIELD2 == 'b' && not(EVENT_FIELD3 == null)", false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_isNotNull_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange(null);
        event_test(seekRange, "EVENT_FIELD2 == 'b' && not(EVENT_FIELD3 == null)", false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        
    }
    
    @Test
    public void event_isNotNullFunction_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        event_test(seekRange, "EVENT_FIELD2 == 'b' && not(filter:isNull(EVENT_FIELD3))", false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void event_isNotNullFunction_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange(null);
        event_test(seekRange, "EVENT_FIELD2 == 'b' && not(filter:isNull(EVENT_FIELD3))", false, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_lazy_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "INDEX_ONLY_FIELD1 == 'apple' && filter:isNotNull(TF_FIELD4@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION)";
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.EMPTY_LIST);
    }
    
    @Test
    public void indexOnly_lazy_shardRange_test() throws IOException {
        Range seekRange = getDocumentRange(null);
        String query = "INDEX_ONLY_FIELD1 == 'apple' && filter:isNotNull(TF_FIELD4@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION)";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    // The term fetched by the delayed context is not added to the returned document.
    @Test
    public void test_fetchDelayedIndexOnlyTerm_addTermToHitTerms() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 == 'a' && ((_Delayed_ = true) && INDEX_ONLY_FIELD1 == 'apple')";
        indexOnly_test(seekRange, query, false, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    }
    
    protected Map.Entry<Key,Map<String,List<String>>> getBaseExpectedEvent(String uid) {
        return getBaseExpectedEvent(DEFAULT_ROW, DEFAULT_DATATYPE, uid);
    }
    
    protected Map.Entry<Key,Map<String,List<String>>> getBaseExpectedEvent(String row, String dataType, String uid) {
        Key hitKey = new Key(row, dataType + Constants.NULL + uid);
        Map<String,List<String>> expectedDocument = new HashMap<>();
        expectedDocument.put("EVENT_FIELD1", Arrays.asList(new String[] {"a"}));
        expectedDocument.put("EVENT_FIELD2", Arrays.asList(new String[] {"b"}));
        expectedDocument.put("EVENT_FIELD3", Arrays.asList(new String[] {"c"}));
        expectedDocument.put("EVENT_FIELD4", Arrays.asList(new String[] {"d"}));
        expectedDocument.put("EVENT_FIELD5", Arrays.asList(new String[] {"e"}));
        expectedDocument.put("EVENT_FIELD6", Arrays.asList(new String[] {"f"}));
        
        // non-normalized form
        expectedDocument.put("TF_FIELD1", Arrays.asList(new String[] {"a,, b,,, c,,"}));
        expectedDocument.put("TF_FIELD2", Arrays.asList(new String[] {",x, ,y, ,z,"}));
        
        return new AbstractMap.SimpleEntry<>(hitKey, expectedDocument);
    }
    
    protected boolean isExpectHitTerm() {
        return options.get(HIT_LIST) != null && Boolean.parseBoolean(options.get(HIT_LIST));
    }
    
    protected void eval(Range seekRange,String query, List<Map.Entry<Key,Map<String,List<String>>>> toEval) throws IOException {


        try {
            var rangeScript = JexlASTHelper.parseJexlQuery(query);

            rangeScript.jjtAccept(iterator, null);

            var sourceIter = iterator.root();
            if (sourceIter != null) {
                sourceIter.initialize();
                sourceIter.setEnvironment(environment);
                var seekableIter = new SeekableNestedIterator(sourceIter, environment);

                seekableIter.seek(seekRange, Collections.EMPTY_LIST, true);
                while (seekableIter.hasNext()) {
                    System.out.println(seekableIter.next());
                }
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
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
    /*
    protected void eval(Key docKeyHit, Map<String,List<String>> docKeys) throws IOException {


        // asserts for a hit or miss
        if (docKeyHit == null) {

            assertFalse(iterator.hasTop());
        } else {
            assertTrue("Expected hit, but got none", iterator.hasTop());
            Key next = iterator.getTopKey();
            assertNotNull(next);
            assertEquals(next.getRow().toString(), docKeyHit.getRow().toString());
            assertEquals(next.getColumnFamily().toString(), docKeyHit.getColumnFamily().toString());
            
            // asserts for document build
            Value topValue = iterator.getTopValue();
            assertNotNull(topValue);
            Map.Entry<Key,Document> deserializedValue = deserialize(topValue);
            assertNotNull(deserializedValue.getValue());
            Document d = deserializedValue.getValue();
            assertNotNull(d);
            
            // -1 is for RECORD_ID field and -1 for HIT_LIST if configured
            int baseSize = d.getDictionary().size() - 1;
            int docSize = isExpectHitTerm() ? baseSize - 1 : baseSize;
            
            assertEquals("Unexpected doc size: " + d.getDictionary().size() + "\nGot: " + docSize + "\n" + "expected: " + docKeys, docKeys.keySet().size(),
                            docSize);
            
            // validate the hitlist
            assertEquals("HIT_TERM presence expected: " + isExpectHitTerm() + " actual: " + (d.getDictionary().get(JexlEvaluation.HIT_TERM_FIELD) != null), (d
                            .getDictionary().get(JexlEvaluation.HIT_TERM_FIELD) != null), isExpectHitTerm());
            
            // verify hits for each specified field
            for (String field : docKeys.keySet()) {
                List<String> expected = docKeys.get(field);
                if (expected.size() == 1) {
                    // verify the only doc
                    Attribute<?> docAttr = d.getDictionary().get(field);
                    if (docAttr instanceof Attributes) {
                        // Special handling of Content attributes, typically when TermFrequencies are looked up.
                        // TFs append Content attributes which results in Attributes coming back instead of a single Attribute
                        Set<?> datas = (Set<?>) docAttr.getData();
                        Set<String> dataStrings = datas.stream().map(Object::toString).collect(Collectors.toSet());
                        boolean stringsMatch = dataStrings.contains(expected.get(0));
                        assertTrue(field + ": value: " + docAttr.getData() + " did not match expected value: " + expected.get(0), stringsMatch);
                    } else {
                        boolean stringsMatch = docAttr.getData().toString().equals(expected.get(0));
                        assertTrue(field + ": value: " + docAttr.getData() + " did not match expected value: " + expected.get(0), stringsMatch);
                    }
                } else {
                    // the data should be a set, verify it matches expected
                    Object dictData = d.getDictionary().get(field).getData();
                    assertNotNull(dictData);
                    assertTrue("Expected " + expected.size() + " values for '" + field + "' found 1, '" + dictData.toString() + "'\nexpected: " + expected,
                                    dictData instanceof Set);
                    Set dictSet = (Set) dictData;
                    assertEquals("Expected " + expected.size() + " values for '" + field + "' found " + dictSet.size() + "\nfound: " + dictSet.toString()
                                    + "\nexpected: " + expected, dictSet.size(), expected.size());
                    Iterator<Attribute> dictIterator = dictSet.iterator();
                    while (dictIterator.hasNext()) {
                        String foundString = dictIterator.next().getData().toString();
                        assertTrue("could not find " + foundString + " in results! Still had " + expected, expected.remove(foundString));
                    }
                    // verify that the expected set is now empty
                    assertEquals(0, expected.size());
                }
            }
            
            // there should be no other hits
            iterator.next();
        }
    }
    */
    private Map.Entry<Key,Document> deserialize(Value value) {
        KryoDocumentDeserializer dser = new KryoDocumentDeserializer();
        return dser.apply(new AbstractMap.SimpleEntry(null, value));
    }
    
    // support methods
    protected Key getTF(String field, String value, String uid, long timestamp) {
        return getTF(DEFAULT_ROW, field, value, DEFAULT_DATATYPE, uid, timestamp);
    }
    
    protected Key getTF(String row, String field, String value, String dataType, String uid, long timestamp) {
        // CQ = dataType\0UID\0Normalized field value\0Field name
        return new Key(row, "tf", dataType + Constants.NULL_BYTE_STRING + uid + Constants.NULL_BYTE_STRING + value + Constants.NULL_BYTE_STRING + field,
                        timestamp);
    }
    
    // Generate TermFrequencyOffsets from a key
    protected Value getTFValue(int position) {
        TermWeight.Info info = TermWeight.Info.newBuilder().addTermOffset(position).addPrevSkips(0)
                        .addScore(TermWeightPosition.positionScoreToTermWeightScore(0.5f)).setZeroOffsetMatch(true).build();
        return new Value(info.toByteArray());
    }
    
    protected Key getFI(String field, String value, String uid, long timestamp) {
        return getFI(DEFAULT_ROW, field, value, DEFAULT_DATATYPE, uid, timestamp);
    }
    
    protected Key getFI(String row, String field, String value, String dataType, String uid, long timestamp) {
        return new Key(row, "fi" + Constants.NULL_BYTE_STRING + field.toUpperCase(), value + Constants.NULL_BYTE_STRING + dataType + Constants.NULL_BYTE_STRING
                        + uid, timestamp);
    }
    
    protected Key getEvent(String field, String value, String uid, long timestamp) {
        return getEvent(DEFAULT_ROW, field, value, DEFAULT_DATATYPE, uid, timestamp);
    }
    
    protected Key getEvent(String row, String field, String value, String dataType, String uid, long timestamp) {
        return new Key(row, dataType + Constants.NULL + uid, field + Constants.NULL + value, timestamp);
    }

    protected GenericObjectPool<SortedKeyValueIterator<Key,Value>> createIvaratorSourcePool(int maxIvaratorSources) {
        return new GenericObjectPool<>(createIvaratorSourceFactory(this), createIvaratorSourcePoolConfig(maxIvaratorSources));
    }

    private BasePoolableObjectFactory<SortedKeyValueIterator<Key,Value>> createIvaratorSourceFactory(SourceFactory<Key,Value> sourceFactory) {
        return new BasePoolableObjectFactory<SortedKeyValueIterator<Key,Value>>() {
            @Override
            public SortedKeyValueIterator<Key,Value> makeObject() throws Exception {
                return sourceFactory.getSourceDeepCopy();
            }
        };
    }

    private GenericObjectPool.Config createIvaratorSourcePoolConfig(int maxIvaratorSources) {
        GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
        poolConfig.maxActive = maxIvaratorSources;
        poolConfig.maxIdle = maxIvaratorSources;
        poolConfig.minIdle = 0;
        poolConfig.whenExhaustedAction = WHEN_EXHAUSTED_BLOCK;
        return poolConfig;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> getSourceDeepCopy() {
        SortedKeyValueIterator<Key,Value> sourceDeepCopy;
        synchronized (baseIterator) {
            sourceDeepCopy = baseIterator.deepCopy(environment);
        }
        return sourceDeepCopy;
    }
}
