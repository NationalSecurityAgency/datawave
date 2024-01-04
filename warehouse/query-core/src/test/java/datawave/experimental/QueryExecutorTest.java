package datawave.experimental;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import datawave.experimental.executor.QueryExecutor;
import datawave.experimental.executor.QueryExecutorOptions;
import datawave.experimental.util.AccumuloUtil;
import datawave.marking.MarkingFunctionsFactory;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.configuration.QueryData;

class QueryExecutorTest {

    protected static AccumuloUtil util;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ExecutorService uidThreadPool = Executors.newFixedThreadPool(25);
    private final ExecutorService documentThreadPool = Executors.newFixedThreadPool(25);

    @BeforeAll
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(QueryExecutorTest.class.getSimpleName());
        util.loadData();

        // setup marking functions once, so it can be shared among all delegate threads
        MarkingFunctionsFactory.createMarkingFunctions();
    }

    @Test
    void testSimpleEq() {
        String query = "FIRST_NAME == 'alice'";
        test(query, util.getAliceUids());
    }

    @Test
    void testMultiEq() {
        String query = "FIRST_NAME == 'alice' && FIRST_NAME == 'bob' && FIRST_NAME == 'eve'";
        Set<String> expectedUids = Sets.intersection(util.getAliceUids(), util.getBobUids());
        expectedUids = Sets.intersection(expectedUids, util.getEveUids());
        test(query, new TreeSet<>(expectedUids));
    }

    @Test
    void testSimpleRegex() {
        String query = "FIRST_NAME =~ 'al.*'";
        test(query, util.getAliceUids());
    }

    @Test
    void testContentPhrase() {
        String query = "content:phrase(TOK, termOffsetMap, 'the', 'message') && TOK == 'the' && TOK == 'message'";
        test(query, new TreeSet<>(List.of("-cjkuoi.9y3aaa.qlnjxw", "-chnv91.hvt1xa.adw8fk", "g744x6.-d6xbm0.-9bgxtu")));
    }

    @Test
    void testContentPhrase2() {
        String query = "content:phrase(TOK, termOffsetMap, 'the', 'message') && TOK == 'the' && TOK == 'message'";
        test(query, new TreeSet<>(List.of("-cjkuoi.9y3aaa.qlnjxw", "-chnv91.hvt1xa.adw8fk", "g744x6.-d6xbm0.-9bgxtu")));
    }

    @Test
    void testContentPhrase3() {
        String query = "content:phrase(TOK, termOffsetMap, 'the', 'message') && TOK == 'the' && TOK == 'message'";
        test(query, new TreeSet<>(List.of("-cjkuoi.9y3aaa.qlnjxw", "-chnv91.hvt1xa.adw8fk", "g744x6.-d6xbm0.-9bgxtu")));
    }

    private void test(String query, Set<String> expectedUids) {

        QueryData data = createQueryData(query);
        ShardQueryConfiguration config = createShardQueryConfig();

        QueryExecutorOptions options = new QueryExecutorOptions();
        options.configureViaQueryData(data);
        options.setAuths(config.getAuthorizations().iterator().next());
        options.setTableName(config.getShardTableName());
        options.setClient(config.getClient());

        // default options
        test(expectedUids, options);

        // uid parallel scan
        options.setUidParallelScan(true);
        test(expectedUids, options);
        options.setUidParallelScan(false);

        // uid sequential scan
        options.setUidSequentialScan(true);
        test(expectedUids, options);
        options.setUidSequentialScan(false);

        // parallel event scan
        options.setDocumentParallelScan(true);
        test(expectedUids, options);
        options.setDocumentParallelScan(false);

        // sequential event scan
        options.setDocumentSequentialScan(true);
        test(expectedUids, options);
        options.setDocumentSequentialScan(false);

        // configured event scan, no parallel or sequential scan
        options.setConfiguredDocumentScan(true);
        test(expectedUids, options);
        options.setConfiguredDocumentScan(false);

        // tf configured scan
        options.setTfConfiguredScan(true);
        test(expectedUids, options);

        // tf configured scan with seeking
        options.setTfSeekingConfiguredScan(true);
        test(expectedUids, options);

        // expected optimal default settings
        options.setUidParallelScan(true);
        // parallel doc
        options.setTfConfiguredScan(true);
        options.setTfSeekingConfiguredScan(true);
        test(expectedUids, options);
    }

    private void test(Set<String> expectedUids, QueryExecutorOptions options) {
        ArrayBlockingQueue<Entry<Key,Value>> results = new ArrayBlockingQueue<>(100);
        QueryExecutor executor = new QueryExecutor(options, null, results, uidThreadPool, documentThreadPool, null);
        Future<?> future = executorService.submit(executor);
        while (!(future.isDone() || future.isCancelled())) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
                fail("Exception while executing QueryExecutor");
            }
        }

        Set<String> uids = new HashSet<>();
        KryoDocumentDeserializer deser = new KryoDocumentDeserializer();
        while (!results.isEmpty()) {
            Document d = deser.apply(results.poll()).getValue();
            Attribute<?> attr = d.get("RECORD_ID");
            String uid = attr.getMetadata().getColumnFamily().toString();
            uid = uid.substring(3);
            uids.add(uid);
        }

        assertEquals(expectedUids, uids);
    }

    private QueryData createQueryData(String query) {
        QueryData data = new QueryData();
        data.setQuery(query);
        data.setRanges(Collections.singleton(createRange()));
        data.setSettings(createSettings());
        return data;
    }

    private Range createRange() {
        Key start = new Key("20201212_0");
        Key end = new Key("20201212_0\u0000");
        return new Range(start, true, end, false);
    }

    private List<IteratorSetting> createSettings() {
        MetadataHelper helper = util.getMetadataHelper();
        IteratorSetting setting = new IteratorSetting(20, "QueryIterator", QueryIterator.class);
        try {
            setting.addOption(QueryOptions.INDEXED_FIELDS, Joiner.on(',').join(helper.getIndexedFields(Collections.emptySet())));
            setting.addOption(QueryOptions.INDEX_ONLY_FIELDS, Joiner.on(',').join(helper.getIndexOnlyFields(Collections.emptySet())));
            setting.addOption(QueryOptions.TERM_FREQUENCY_FIELDS, Joiner.on(',').join(helper.getTermFrequencyFields(Collections.emptySet())));
            setting.addOption(QueryOptions.TYPE_METADATA, helper.getTypeMetadata().toString());
            setting.addOption(QueryOptions.PROJECTION_FIELDS, "FIRST_NAME,EVENT_ONLY");
            setting.addOption(QueryOptions.DISALLOWLISTED_FIELDS, "MSG_SIZE");
        } catch (Exception e) {
            fail("failed to load iterator settings for test");
        }
        return Collections.singletonList(setting);
    }

    private ShardQueryConfiguration createShardQueryConfig() {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setClient(util.getClient());
        config.setAuthorizations(Collections.singleton(util.getAuths()));
        return config;
    }

}
