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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.jboss.weld.executor.SingleThreadExecutorServices;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import datawave.experimental.executor.QueryExecutor;
import datawave.experimental.executor.QueryExecutorOptions;
import datawave.experimental.threads.DocumentUncaughtExceptionHandler;
import datawave.experimental.threads.NamedThreadFactory;
import datawave.experimental.threads.UidUncaughtExceptionHandler;
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

    private final ExecutorService executorService = createExecutorService();

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

    // bounded range 0 to 10
    @Test
    void testBoundedRange() {
        String query = "((_Bounded_ = true) && (MSG_SIZE > '+AE0' && MSG_SIZE < '+bE1'))";
        test(query, util.getMessageUids());
    }

    @Test
    void testBoundedRangeAndTerm() {
        String query = "((_Bounded_ = true) && (MSG_SIZE > '+AE0' && MSG_SIZE < '+bE1')) && FIRST_NAME == 'bob'";
        test(query, Sets.intersection(util.getMessageUids(), util.getBobUids()));
    }

    @Test
    void testFilterFunctionIncludeRegex() {
        String query = "FIRST_NAME == 'alice' && filter:includeRegex(FIRST_NAME,'bo.*')";
        test(query, Sets.intersection(util.getAliceUids(), util.getBobUids()));
    }

    @Test
    void testFilterFunctionExcludeRegex() {
        String query = "FIRST_NAME == 'alice' && filter:excludeRegex(FIRST_NAME,'bo.*')";
        test(query, Sets.difference(util.getAliceUids(), util.getBobUids()));
    }

    @Test
    void testQueryFunctionIncludeText() {
        String query = "FIRST_NAME == 'alice' && f:includeText(FIRST_NAME,'bob')";
        test(query, Sets.intersection(util.getAliceUids(), util.getBobUids()));
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
        // test(expectedUids, options);

        // uid parallel scan
        options.setUidParallelScan(true);
        test(expectedUids, options);
        options.setUidParallelScan(false);

        // uid sequential scan
        options.setUidSequentialScan(true);
        test(expectedUids, options);
        options.setUidSequentialScan(false);

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

        final int threads = 5;
        // mirror remote scheduler threading setup
        UidUncaughtExceptionHandler uidHandler = new UidUncaughtExceptionHandler();
        NamedThreadFactory uidThreadFactory = new NamedThreadFactory("UidThreadFactory", uidHandler);
        ExecutorService uidThreadPool = new ThreadPoolExecutor(threads, threads, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), uidThreadFactory);
        uidThreadPool = MoreExecutors.listeningDecorator(uidThreadPool);

        DocumentUncaughtExceptionHandler docHandler = new DocumentUncaughtExceptionHandler();
        NamedThreadFactory docThreadFactory = new NamedThreadFactory("DocThreadFactory", docHandler);
        ExecutorService documentThreadPool = new ThreadPoolExecutor(threads, threads, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), docThreadFactory);
        documentThreadPool = MoreExecutors.listeningDecorator(documentThreadPool);

        QueryExecutor executor = new QueryExecutor(options, null, results, uidThreadPool, documentThreadPool, null);
        ListenableFuture<QueryExecutor> future = (ListenableFuture<QueryExecutor>) executorService.submit(executor);
        Futures.addCallback(future, new QueryExecutorCallback(), executorService);
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
            setting.addOption(QueryOptions.PROJECTION_FIELDS, "FIRST_NAME,EVENT_ONLY,MSG_SIZE");
            setting.addOption(QueryOptions.DISALLOWLISTED_FIELDS, "TOK"); // tokenized non-event field. Should be fine for tests. Need an event field that
                                                                          // doesn't matter.
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

    private ExecutorService createExecutorService() {
        return MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
    }

    private static class QueryExecutorCallback implements FutureCallback<QueryExecutor> {
        @Override
        public void onSuccess(QueryExecutor result) {

        }

        @Override
        public void onFailure(Throwable t) {
            System.out.println("problem: " + t.getMessage());
            Throwables.throwIfUnchecked(t);
        }
    }

}
