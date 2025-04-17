package datawave.query.iterator;

import static datawave.query.iterator.QueryOptions.ALLOW_FIELD_INDEX_EVALUATION;
import static datawave.query.iterator.QueryOptions.ALLOW_TERM_FREQUENCY_LOOKUP;
import static datawave.query.iterator.QueryOptions.CONTAINS_INDEX_ONLY_TERMS;
import static datawave.query.iterator.QueryOptions.END_TIME;
import static datawave.query.iterator.QueryOptions.HDFS_SITE_CONFIG_URLS;
import static datawave.query.iterator.QueryOptions.HIT_LIST;
import static datawave.query.iterator.QueryOptions.INDEXED_FIELDS;
import static datawave.query.iterator.QueryOptions.INDEX_ONLY_FIELDS;
import static datawave.query.iterator.QueryOptions.IVARATOR_CACHE_DIR_CONFIG;
import static datawave.query.iterator.QueryOptions.NON_INDEXED_DATATYPES;
import static datawave.query.iterator.QueryOptions.QUERY;
import static datawave.query.iterator.QueryOptions.QUERY_ID;
import static datawave.query.iterator.QueryOptions.SCAN_ID;
import static datawave.query.iterator.QueryOptions.SERIAL_EVALUATION_PIPELINE;
import static datawave.query.iterator.QueryOptions.START_TIME;
import static datawave.query.iterator.QueryOptions.TERM_FREQUENCIES_REQUIRED;
import static datawave.query.iterator.QueryOptions.TERM_FREQUENCY_FIELDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.util.TypeMetadata;

/**
 * Integration tests for the QueryIterator
 *
 */
public class QueryIteratorIT extends EasyMockSupport {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected QueryIterator iterator;
    protected SortedListKeyValueIterator baseIterator;
    protected Map<String,String> options;
    protected IteratorEnvironment iterEnv;
    protected PluginEnvironment pluginEnv;
    protected EventDataQueryFilter filter;
    protected TypeMetadata typeMetadata;

    // Default row is for day 20190314, shard 0
    protected static final String DEFAULT_ROW = "20190314_0";

    // Default if test does not specify a datatype
    protected static final String DEFAULT_DATATYPE = "dataType1";

    public Path tempPath;

    protected Set<String> indexedFields = Set.of("EVENT_FIELD1", "EVENT_FIELD4", "EVENT_FIELD6", "TF_FIELD0", "TF_FIELD1", "TF_FIELD2", "INDEX_ONLY_FIELD1",
                    "INDEX_ONLY_FIELD2", "INDEX_ONLY_FIELD3");
    protected Set<String> indexOnlyFields = Set.of("TF_FIELD0", "TF_FIELD1", "TF_FIELD2", "INDEX_ONLY_FIELD1", "INDEX_ONLY_FIELD2", "INDEX_ONLY_FIELD3");
    protected Set<String> termFrequencyFields = Set.of("TF_FIELD0", "TF_FIELD1", "TF_FIELD2");

    protected static final Value EMPTY_VALUE = new Value();

    @Before
    public void setup() throws IOException {
        iterator = new QueryIterator();
        options = new HashMap<>();
        tempPath = temporaryFolder.newFolder().toPath();

        // global options

        // force serial pipelines
        options.put(SERIAL_EVALUATION_PIPELINE, "true");
        options.put(ALLOW_FIELD_INDEX_EVALUATION, "true");
        options.put(ALLOW_TERM_FREQUENCY_LOOKUP, "true");

        // set the indexed fields list
        options.put(INDEXED_FIELDS, Joiner.on(',').join(indexedFields));
        options.put(INDEX_ONLY_FIELDS, Joiner.on(',').join(indexOnlyFields));
        options.put(TERM_FREQUENCY_FIELDS, Joiner.on(',').join(termFrequencyFields));

        // set the unindexed fields list
        Multimap<String,Type<?>> nonIndexedQueryFieldsDatatypes = HashMultimap.create();
        nonIndexedQueryFieldsDatatypes.put("EVENT_FIELD2", new LcNoDiacriticsType());
        nonIndexedQueryFieldsDatatypes.put("EVENT_FIELD3", new LcNoDiacriticsType());
        nonIndexedQueryFieldsDatatypes.put("EVENT_FIELD5", new LcNoDiacriticsType());
        String nonIndexedTypes = QueryOptions.buildFieldNormalizerString(nonIndexedQueryFieldsDatatypes);
        options.put(NON_INDEXED_DATATYPES, nonIndexedTypes);

        // set a query id
        options.put(QUERY_ID, "000001");

        // set a scanId
        options.put(SCAN_ID, "000002");

        // setup ivarator settings
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig("file://" + tempPath.toAbsolutePath());
        options.put(IVARATOR_CACHE_DIR_CONFIG, IvaratorCacheDirConfig.toJson(config));
        URL hdfsSiteConfig = this.getClass().getResource("/testhadoop.config");
        Preconditions.checkNotNull(hdfsSiteConfig);
        options.put(HDFS_SITE_CONFIG_URLS, hdfsSiteConfig.toExternalForm());

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

        iterEnv = createMock(IteratorEnvironment.class);
        pluginEnv = createMock(PluginEnvironment.class);
        EasyMock.expect(iterEnv.getConfig()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
        EasyMock.expect(iterEnv.getPluginEnv()).andReturn(pluginEnv).anyTimes();
        EasyMock.expect(pluginEnv.getConfiguration()).andReturn(new ConfigurationImpl(DefaultConfiguration.getInstance())).anyTimes();
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
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD1", "a", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "EVENT_FIELD1", "a", dataType, uid, eventTime), EMPTY_VALUE));
        // unindexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD2", "b", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD3", "c", dataType, uid, eventTime), EMPTY_VALUE));
        // indexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD4", "d", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "EVENT_FIELD4", "d", dataType, uid, eventTime), EMPTY_VALUE));
        // unindexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD5", "e", dataType, uid, eventTime), EMPTY_VALUE));
        // indexed
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "EVENT_FIELD6", "f", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "EVENT_FIELD6", "f", dataType, uid, eventTime), EMPTY_VALUE));

        // add some indexed TF fields
        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(DEFAULT_ROW, "TF_FIELD1", "a,, b,,, c,,", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD1", "a b c", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD1", "a", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD1", "b", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD1", "c", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD1", "a", dataType, uid, eventTime), getTFValue(0)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD1", "b", dataType, uid, eventTime), getTFValue(1)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD1", "c", dataType, uid, eventTime), getTFValue(2)));

        listSource.add(new AbstractMap.SimpleEntry<>(getEvent(row, "TF_FIELD2", ",x, ,y, ,z,", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD2", "x y z", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD2", "x", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD2", "y", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD2", "z", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD2", "x", dataType, uid, eventTime), getTFValue(23)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD2", "y", dataType, uid, eventTime), getTFValue(24)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD2", "z", dataType, uid, eventTime), getTFValue(25)));

        // add an index only TF
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "TF_FIELD4", "d", dataType, uid, eventTime), getTFValue(3)));
        listSource.add(new AbstractMap.SimpleEntry<>(getTF(row, "TF_FIELD4", "d", dataType, uid, eventTime), getTFValue(3)));

        // add some index only field data
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD1", "apple", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD1", "pear", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD1", "orange", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD2", "beef", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD2", "chicken", dataType, uid, eventTime), EMPTY_VALUE));
        listSource.add(new AbstractMap.SimpleEntry<>(getFI(row, "INDEX_ONLY_FIELD2", "pork", dataType, uid, eventTime), EMPTY_VALUE));

        return listSource;
    }

    protected List<Map.Entry<Key,Value>> configureTestData(long eventTime) {
        return new ArrayList<>(addEvent(eventTime, "123.345.456"));
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
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_documentSpecific_intersection_noExtraSourceDeepCopies_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "INDEX_ONLY_FIELD1 == 'apple' && INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_shardRange_intersection_noExtraSourceDeepCopies_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "INDEX_ONLY_FIELD1 == 'apple' && INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_documentSpecific_union_noExtraSourceDeepCopies_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "INDEX_ONLY_FIELD1 == 'apple' || INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_shardRange_union_noExtraSourceDeepCopies_test() throws IOException {
        // build the seek range for a shard
        Range seekRange = getShardRange();
        String query = "INDEX_ONLY_FIELD1 == 'apple' || INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_documentSpecific_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_shardRange_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a shard
        Range seekRange = getShardRange();
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        indexOnly_test(seekRange, "INDEX_ONLY_FIELD1 == 'fork'", true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "INDEX_ONLY_FIELD1 == 'fork'";
        indexOnly_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    @Test
    public void indexOnly_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "INDEX_ONLY_FIELD1 == 'apple'";
        Map.Entry<Key,Map<String,List<String>>> secondEvent = getBaseExpectedEvent("123.345.457");
        secondEvent.getValue().put("INDEX_ONLY_FIELD1", List.of("apple"));
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), List.of(secondEvent));
    }

    @Test
    public void indexOnly_trailingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_trailingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_trailingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'f.*'))";
        indexOnly_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_trailingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'f.*'))";
        indexOnly_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_trailingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))";
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    @Test
    public void indexOnly_trailingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ 'ap.*'))";
        Map.Entry<Key,Map<String,List<String>>> secondEvent = getBaseExpectedEvent("123.345.457");
        secondEvent.getValue().put("INDEX_ONLY_FIELD1", List.of("apple"));
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), List.of(secondEvent));
    }

    @Test
    public void indexOnly_leadingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_leadingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_leadingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*k'))";
        indexOnly_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_leadingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*k'))";
        indexOnly_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_leadingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))";
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    @Test
    public void indexOnly_leadingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (INDEX_ONLY_FIELD1 =~ '.*le'))";
        Map.Entry<Key,Map<String,List<String>>> secondEvent = getBaseExpectedEvent("123.345.457");
        secondEvent.getValue().put("INDEX_ONLY_FIELD1", List.of("apple"));
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), List.of(secondEvent));
    }

    @Test
    public void event_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_documentSpecific_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_shardRange_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 == 'a'";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 == 'a'";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    @Test
    public void event_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 == 'b'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Collections.singletonList(getBaseExpectedEvent("123.345.457")));
    }

    @Test
    public void event_trailingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ 'b.*'";
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_trailingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ 'b.*'";
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_trailingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ 'a.*'";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_trailingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ 'a.*'";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_trailingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ 'b.*'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    @Test
    public void event_trailingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ 'b.*'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Collections.singletonList(getBaseExpectedEvent("123.345.457")));
    }

    @Test
    public void event_leadingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ '.*b'";
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_leadingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ '.*b'";
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_leadingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ '.*a'";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_leadingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ '.*a'";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_leadingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD2 =~ '.*b'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    @Test
    public void event_leadingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD2 =~ '.*b'";
        event_test(seekRange, query, false, null, addEvent(11, "123.345.457"), Collections.singletonList(getBaseExpectedEvent("123.345.457")));
    }

    @Test(expected = IOException.class)
    public void event_bogusIvaratorCacheDir_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*d'))";

        // setup a bogus ivarator cache dir for the config
        options.put(IVARATOR_CACHE_DIR_CONFIG, IvaratorCacheDirConfig.toJson(new IvaratorCacheDirConfig("hddfs://bogusPath")));

        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_documentSpecific_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_shardRange_hitTerm_test() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD4 == 'e'";
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD4 == 'e'";
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    /**
     * Doc specific range should not find the second document
     *
     * @throws IOException
     *             for issues with read/write
     */
    @Test
    public void index_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    /**
     * Shard range should find the second document
     *
     * @throws IOException
     *             for issues with read/write
     */
    @Test
    public void index_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD4 == 'd'";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.singletonList(getBaseExpectedEvent("123.345.457")));
    }

    @Test
    public void index_trailingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'd.*'))";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_trailingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'd.*'))";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_trailingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'e.*'))";
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_trailingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'e.*'))";
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_trailingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'd.*'))";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    @Test
    public void index_trailingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ 'd.*'))";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.singletonList(getBaseExpectedEvent("123.345.457")));
    }

    @Test
    public void index_leadingRegex_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*d'))";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_leadingRegex_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*d'))";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_leadingRegex_documentSpecific_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*e'))";
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_leadingRegex_shardRange_miss_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*e'))";
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void index_leadingRegex_documentSpecific_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*d'))";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    @Test
    public void index_leadingRegex_shardRange_secondEvent_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "((_Value_ = true) && (EVENT_FIELD4 =~ '.*d'))";
        index_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.singletonList(getBaseExpectedEvent("123.345.457")));
    }

    @Test
    public void tf_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ 'b.*'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_negation_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD2 !~ 'b.*'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    // alternate form of negation
    @Test
    public void tf_negation2_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 =~ 'b.*'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_index_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD2 !~ 'y.*'))";
        // note: correcting this marker caused the query to miss
        // this behavior is correct because the document {EVENT_FIELD1=a TF_FIELD2=y} does not satisfy the query
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    // alternate form of negation
    @Test
    public void tf_index2_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 =~ 'y.*'))";
        // note: correcting this marker caused the query to miss
        // this behavior is correct because the document {EVENT_FIELD1=a TF_FIELD2=y} does not satisfy the query
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_event_exceededValue_trailingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ 'b.*'))";
        // note: correcting this marker caused the query to hit
        // this behavior is correct because the document {EVENT_FIELD1=a TF_FIELD1=b} satisfies the query
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ 'b.*'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_negation_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 !~ 'b.*'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_index_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 !~ 'z.*'))";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_negation2_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ 'b.*'))";
        tf_test(seekRange, query, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_index2_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ 'z.*'))";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_event_exceededValue_trailingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ 'b.*'))";
        // note: correcting the marker caused this query to hit
        // this behavior is correct because the document {EVENT_FIELD1=a TF_FIELD1=b} satisfies the query
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_leadingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_negation_exceededValue_leadingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD2 !~ '.*b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_index_exceededValue_leadingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD2 !~ '.*x'))";
        // note: correcting this marker caused the query to miss
        // this behavior is correct because the document {EVENT_FIELD1=a TF_FIELD2=x} does not satisfy the query
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    // alternate form of negation
    @Test
    public void tf_negation2_exceededValue_leadingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 =~ '.*b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    // alternate form of negation
    @Test
    public void tf_index2_exceededValue_leadingWildcard_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 =~ '.*x'))";
        // note: correcting this marker caused the query to miss
        // this behavior is correct because the document {EVENT_FIELD1=a TF_FIELD2=x} does not satisfy the query
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_negation_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 !~ '.*b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_index_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 !~ '.*a'))";
        // note: correcting the marker caused this query to miss
        // this behavior is correct because the document {EVENT_FIELD1=a TF_FIELD1=a} does not satisfy the query
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    // alternate form of negation
    @Test
    public void tf_negation2_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*b'))";
        tf_test(seekRange, query, null, Collections.emptyList(), Collections.emptyList());
    }

    // alternate form of negation
    @Test
    public void tf_index2_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*a'))";
        // note: correcting the marker caused this query to miss
        // this behavior is correct because the document {EVENT_FIELD1=a TF_FIELD1=a} does not satisfy the query
        index_test(seekRange, query, true, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_event_exceededValue_leadingWildcard_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((_Value_ = true) && (TF_FIELD1 =~ '.*b'))";
        // note: correcting the marker caused this query to hit
        // this behavior is correct because the document {EVENT_FIELD1=a TF_FIELD1=b} satisfies the query
        event_test(seekRange, query, false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_negated_leadingWildcard_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_negation_exceededValue_negated_leadingWildcard_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 !~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_index_exceededValue_negated_leadingWildcard_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 !~ '.*z'))";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_event_exceededValue_negated_leadingWildcard_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 =~ '.*z'))";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_negated_leadingWildcard_multiTerm_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*b c'))";
        tf_test(seekRange, query, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_negated_leadingWildcard_multiTerm_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*b c'))";
        tf_test(seekRange, query, null, Collections.emptyList(), Collections.emptyList());

    }

    @Test
    public void tf_exceededValue_negated_leadingWildcard_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD1 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_event_exceededValue_negated_leadingWildcard_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 =~ '.*z'))";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_negated_leadingWildcardMissIndexOnly_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD4 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_negation_exceededValue_negated_leadingWildcardMissIndexOnly_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 !~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_index_exceededValue_negated_leadingWildcardMissIndexOnly_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 !~ '.*z'))";
        index_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_event_exceededValue_negated_leadingWildcardMissIndexOnly_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 =~ '.*z'))";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_exceededValue_negated_leadingWildcardMissIndexOnly_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD4 =~ '.*z'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    // terms 'a' and 'b' are adjacent, thus a valid phrase
    @Test
    public void tf_contentFunction_validPhrase_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((TF_FIELD1 =='a' && TF_FIELD1 =='b') && content:phrase(TF_FIELD1,termOffsetMap,'a','b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    // terms 'a' and 'c' do not appear adjacent
    @Test
    public void tf_contentFunction_invalidPhrase_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && ((TF_FIELD1 =='a' && TF_FIELD1 =='c') && content:phrase(TF_FIELD1,termOffsetMap,'a','c'))";
        tf_test(seekRange, query, new AbstractMap.SimpleEntry<>(null, null), Collections.emptyList(), Collections.emptyList());
    }

    // terms 'a' and 'b' are adjacent, thus a valid phrase
    @Test
    public void tf_contentFunction_validPhrase_docRange_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((TF_FIELD1 =='a' && TF_FIELD1 =='b') && content:phrase(TF_FIELD1,termOffsetMap,'a','b'))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    // terms 'a' and 'c' do not appear adjacent
    @Test
    public void tf_contentFunction_invalidPhrase_docRange_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && ((TF_FIELD1 =='a' && TF_FIELD1 =='c') && content:phrase(TF_FIELD1,termOffsetMap,'a','c'))";
        tf_test(seekRange, query, new AbstractMap.SimpleEntry<>(null, null), Collections.emptyList(), Collections.emptyList());
    }

    // A && (phrase:'a,b' || phrase:'j,k')
    // Second phrase for TF_FIELD0 that does not appear in the document should be pruned from the TF FieldValues multimap.
    @Test
    public void tf_contentFunction_validPhrase_docRange_test2() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 =='a' && (((TF_FIELD1 =='a' && TF_FIELD1 =='b') && content:phrase(TF_FIELD1,termOffsetMap,'a','b')) || "
                        + "((TF_FIELD0 =='j' && TF_FIELD0 =='k') && content:phrase(TF_FIELD0,termOffsetMap,'j','k')))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    // A && (phrase:'a,b' || phrase:'j,k')
    // Second phrase for TF_FIELD0 that does not appear in the document should be pruned from the TF FieldValues multimap.
    @Test
    public void tf_contentFunction_validPhrase_shardRange_test3() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && (((TF_FIELD1 =='a' && TF_FIELD1 =='b') && content:phrase(TF_FIELD1,termOffsetMap,'a','b')) || "
                        + "((TF_FIELD0 =='j' && TF_FIELD0 =='k') && content:phrase(TF_FIELD0,termOffsetMap,'j','k')))";
        tf_test(seekRange, query, getBaseExpectedEvent("123.345.456"), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void tf_event_exceededValue_negated_leadingWildcardMissIndexOnly_shardRange_test() throws IOException {
        Range seekRange = getShardRange();
        String query = "EVENT_FIELD1 =='a' && !((_Value_ = true) && (TF_FIELD2 =~ '.*z'))";
        event_test(seekRange, query, true, null, Collections.emptyList(), Collections.emptyList());
    }

    protected void configureIterator() {
        // configure iterator
        iterator.setEvaluationFilter(filter);
        iterator.setTypeMetadata(typeMetadata);
    }

    /**
     * Simulate a full table scan against an event data (only) query
     *
     * @param seekRange
     *            the seek range
     * @throws IOException
     *             IOException for issues with read/write
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

        replayAll();

        iterator.init(baseIterator, options, iterEnv);
        iterator.seek(seekRange, Collections.emptyList(), true);

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
        eval(hits);
    }

    /**
     * Simulate an indexed query
     *
     * @param seekRange
     *            the seek range
     * @throws IOException
     *             IOException for issues with read/write
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

        iterator.init(baseIterator, options, iterEnv);
        iterator.seek(seekRange, Collections.emptyList(), true);

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
     *            the seek range
     * @throws IOException
     *             IOException for issues with read/write
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
        options.put(TERM_FREQUENCIES_REQUIRED, "true");
        // set to be the index only fields required for the query?
        options.put(INDEX_ONLY_FIELDS, "INDEX_ONLY_FIELD1,INDEX_ONLY_FIELD2,INDEX_ONLY_FIELD3,TF_FIELD4");
        // set because we have index only fields used
        options.put(CONTAINS_INDEX_ONLY_TERMS, "true");

        replayAll();

        iterator.init(baseIterator, options, iterEnv);
        iterator.seek(seekRange, Collections.emptyList(), true);

        verifyAll();

        List<Map.Entry<Key,Map<String,List<String>>>> hits = new ArrayList<>();
        if (miss) {
            hits.add(new AbstractMap.SimpleEntry<>(null, null));
        } else {
            // define the base hit
            Map.Entry<Key,Map<String,List<String>>> hit = getBaseExpectedEvent("123.345.456");
            hit.getValue().put("INDEX_ONLY_FIELD1", List.of("apple"));
            hits.add(hit);
        }

        hits.addAll(otherHits);
        eval(hits);
    }

    /**
     * Simulate a TF query
     *
     * @throws IOException
     *             IOException for issues with read/write
     */
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

        iterator.init(baseIterator, options, iterEnv);
        iterator.seek(seekRange, Collections.emptyList(), true);

        verifyAll();

        List<Map.Entry<Key,Map<String,List<String>>>> hits = new ArrayList<>();
        if (hit != null) {
            hits.add(hit);
        }

        hits.addAll(otherHits);
        eval(hits);
    }

    @Test
    public void event_isNotNull_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        event_test(seekRange, "EVENT_FIELD2 == 'b' && not(EVENT_FIELD3 == null)", false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_isNotNull_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange(null);
        event_test(seekRange, "EVENT_FIELD2 == 'b' && not(EVENT_FIELD3 == null)", false, null, Collections.emptyList(), Collections.emptyList());

    }

    @Test
    public void event_isNotNullFunction_documentSpecific_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        event_test(seekRange, "EVENT_FIELD2 == 'b' && filter:isNotNull(EVENT_FIELD3)", false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void event_isNotNullFunction_shardRange_test() throws IOException {
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange(null);
        event_test(seekRange, "EVENT_FIELD2 == 'b' && filter:isNotNull(EVENT_FIELD3)", false, null, Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void indexOnly_lazy_documentSpecific_test() throws IOException {
        Range seekRange = getDocumentRange("123.345.456");
        String query = "INDEX_ONLY_FIELD1 == 'apple' && filter:isNotNull(TF_FIELD4@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION)";
        indexOnly_test(seekRange, query, false, addEvent(11, "123.345.457"), Collections.emptyList());
    }

    @Test
    public void indexOnly_lazy_shardRange_test() throws IOException {
        Range seekRange = getDocumentRange(null);
        String query = "INDEX_ONLY_FIELD1 == 'apple' && filter:isNotNull(TF_FIELD4@LAZY_SET_FOR_INDEX_ONLY_FUNCTION_EVALUATION)";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    // The term fetched by the delayed context is not added to the returned document.
    @Test
    public void test_fetchDelayedIndexOnlyTerm_addTermToHitTerms() throws IOException {
        options.put(JexlEvaluation.HIT_TERM_FIELD, "true");
        // build the seek range for a document specific pull
        Range seekRange = getDocumentRange("123.345.456");
        String query = "EVENT_FIELD1 == 'a' && ((_Delayed_ = true) && INDEX_ONLY_FIELD1 == 'apple')";
        indexOnly_test(seekRange, query, false, Collections.emptyList(), Collections.emptyList());
    }

    protected Map.Entry<Key,Map<String,List<String>>> getBaseExpectedEvent(String uid) {
        return getBaseExpectedEvent(DEFAULT_ROW, DEFAULT_DATATYPE, uid);
    }

    protected Map.Entry<Key,Map<String,List<String>>> getBaseExpectedEvent(String row, String dataType, String uid) {
        Key hitKey = new Key(row, dataType + Constants.NULL + uid);
        Map<String,List<String>> expectedDocument = new HashMap<>();
        expectedDocument.put("EVENT_FIELD1", List.of("a"));
        expectedDocument.put("EVENT_FIELD2", List.of("b"));
        expectedDocument.put("EVENT_FIELD3", List.of("c"));
        expectedDocument.put("EVENT_FIELD4", List.of("d"));
        expectedDocument.put("EVENT_FIELD5", List.of("e"));
        expectedDocument.put("EVENT_FIELD6", List.of("f"));

        // non-normalized form
        expectedDocument.put("TF_FIELD1", List.of("a,, b,,, c,,"));
        expectedDocument.put("TF_FIELD2", List.of(",x, ,y, ,z,"));

        return new AbstractMap.SimpleEntry<>(hitKey, expectedDocument);
    }

    protected boolean isExpectHitTerm() {
        return options.get(HIT_LIST) != null && Boolean.parseBoolean(options.get(HIT_LIST));
    }

    protected void eval(List<Map.Entry<Key,Map<String,List<String>>>> toEval) throws IOException {
        for (Map.Entry<Key,Map<String,List<String>>> evalPair : toEval) {
            eval(evalPair.getKey(), evalPair.getValue());
        }
        assertFalse(iterator.hasTop());
    }

    /**
     * Deserialize and evaluate the document, expects 0 to 1 documents
     *
     * @param docKeyHit
     *            the expected hit Key, null if no hit expected
     * @param docKeys
     *            the expected values
     * @throws IOException
     *             IOException for issues with read/write
     */
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

            // validate the hit list
            assertEquals("HIT_TERM presence expected: " + isExpectHitTerm() + " actual: " + (d.getDictionary().get(JexlEvaluation.HIT_TERM_FIELD) != null),
                            (d.getDictionary().get(JexlEvaluation.HIT_TERM_FIELD) != null), isExpectHitTerm());

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
                    assertTrue("Expected " + expected.size() + " values for '" + field + "' found 1, '" + dictData + "'\nexpected: " + expected,
                                    dictData instanceof Set);
                    Set<?> dictSet = (Set<?>) dictData;
                    assertEquals("Expected " + expected.size() + " values for '" + field + "' found " + dictSet.size() + "\nfound: " + dictSet + "\nexpected: "
                                    + expected, dictSet.size(), expected.size());
                    for (Attribute<?> attribute : (Iterable<Attribute<?>>) dictSet) {
                        String foundString = attribute.getData().toString();
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

    private final KryoDocumentDeserializer documentDeserializer = new KryoDocumentDeserializer();

    private Map.Entry<Key,Document> deserialize(Value value) {
        return documentDeserializer.apply(new AbstractMap.SimpleEntry<>(null, value));
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
        return new Key(row, "fi" + Constants.NULL_BYTE_STRING + field.toUpperCase(),
                        value + Constants.NULL_BYTE_STRING + dataType + Constants.NULL_BYTE_STRING + uid, timestamp);
    }

    protected Key getEvent(String field, String value, String uid, long timestamp) {
        return getEvent(DEFAULT_ROW, field, value, DEFAULT_DATATYPE, uid, timestamp);
    }

    protected Key getEvent(String row, String field, String value, String dataType, String uid, long timestamp) {
        return new Key(row, dataType + Constants.NULL + uid, field + Constants.NULL + value, timestamp);
    }
}
