package datawave.experimental.executor;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import datawave.experimental.fi.ParallelUidScanner;
import datawave.experimental.fi.SerialUidScanner;
import datawave.experimental.fi.UidScanner;
import datawave.experimental.scanner.FieldIndexScanner;
import datawave.experimental.scanner.event.ConfiguredEventScanner;
import datawave.experimental.scanner.event.DefaultEventScanner;
import datawave.experimental.scanner.event.EventScanner;
import datawave.experimental.scanner.tf.TermFrequencyConfiguredScanner;
import datawave.experimental.scanner.tf.TermFrequencyScanner;
import datawave.experimental.scanner.tf.TermFrequencySequentialScanner;
import datawave.experimental.threads.EnrichmentThread;
import datawave.experimental.threads.EvaluationThread;
import datawave.experimental.threads.EventAggregationThread;
import datawave.experimental.threads.FutureCompleteThread;
import datawave.experimental.util.ScanStats;
import datawave.experimental.visitor.QueryTermVisitor;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttributeFactory;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.predicate.TimeFilter;
import datawave.query.util.MetadataHelper;
import datawave.query.util.Tuple3;

/**
 * Handles running a query via remote scans. Below is an overview of the execution pipeline.
 * <ol>
 * <li>Query is parsed and touched up</li>
 * <li>Document keys are pulled from the field index</li>
 * <li>IndexOnly fields are aggregated</li>
 * <li>TermFrequency fields are aggregated</li>
 * <li>Document is pushed to document queue</li>
 * <li>Documents are evaluated and pushed onto the result queue</li>
 * </ol>
 */
public class QueryExecutor implements Runnable {

    private static final Logger log = Logger.getLogger(QueryExecutor.class);

    private final String tableName;
    private final Authorizations auths;
    private final AccumuloClient client;
    private final MetadataHelper metadataHelper;

    private final QueryExecutorOptions options;

    private final PreNormalizedAttributeFactory preNormalizedAttributeFactory;
    private final AttributeFactory attributeFactory;

    private final LinkedBlockingQueue<String> uidQueue;
    private final LinkedBlockingQueue<Document> eventQueue;
    private final LinkedBlockingQueue<Tuple3<Key,Document,DatawaveJexlContext>> candidateQueue;
    private final LinkedBlockingQueue<Entry<Key,Document>> documentQueue;
    private final BlockingQueue<Entry<Key,Value>> resultQueue;

    private final ExecutorService uidThreadPool;
    private final ExecutorService eventThreadPool;
    private final ExecutorService documentThreadPool;

    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();

    private final FieldIndexScanner fiScanner;
    private final EventScanner eventScanner;
    private final TermFrequencyScanner tfScanner;
    private final Set<String> indexOnlyFields;
    private final Set<JexlNode> terms;
    private final Set<String> tfFields;
    private final String shard;

    private long setupTime;

    private final int numEventThreads = 2;
    private final int numEvaluationThreads = 1;

    //
    private ExecutorService workerThreads;

    private ScanStats stats;
    private ScanStats globalStats;

    public QueryExecutor(QueryExecutorOptions options, MetadataHelper metadataHelper, BlockingQueue<Entry<Key,Value>> results, ExecutorService uidThreadPool,
                    ExecutorService eventThreadPool, ExecutorService documentThreadPool, ScanStats globalStats) {
        setupTime = System.currentTimeMillis();

        this.options = options;
        this.client = options.getClient();
        this.tableName = options.getTableName();
        this.auths = options.getAuths();
        this.metadataHelper = metadataHelper;

        this.preNormalizedAttributeFactory = new PreNormalizedAttributeFactory(options.getTypeMetadata());
        this.attributeFactory = new AttributeFactory(options.getTypeMetadata());

        this.uidQueue = new LinkedBlockingQueue<>(); // unbounded
        this.eventQueue = new LinkedBlockingQueue<>(50);
        this.candidateQueue = new LinkedBlockingQueue<>(50);
        this.documentQueue = new LinkedBlockingQueue<>(50);
        this.resultQueue = results;

        this.uidThreadPool = uidThreadPool;
        this.eventThreadPool = eventThreadPool;
        this.documentThreadPool = documentThreadPool;

        this.fiScanner = getFieldIndexScanner();
        this.eventScanner = getEventScanner();
        this.tfScanner = getTfScanner();

        // up front check for index only and term frequency fields
        Set<String> queryFields = JexlASTHelper.getIdentifierNames(options.getScript());
        this.indexOnlyFields = Sets.intersection(queryFields, options.getIndexOnlyFields());
        this.tfFields = Sets.intersection(queryFields, options.getTermFrequencyFields());
        this.terms = QueryTermVisitor.parse(options.getScript());

        this.shard = options.getRange().getStartKey().getRow().toString();

        // local thread pool that handles event aggregation
        this.workerThreads = Executors.newFixedThreadPool(numEventThreads + numEvaluationThreads + 1);
        this.workerThreads = MoreExecutors.listeningDecorator(workerThreads);

        if (options.isStatsEnabled()) {
            this.globalStats = globalStats;
            this.stats = new ScanStats();
        }

        setupTime = System.currentTimeMillis() - setupTime;
    }

    private FieldIndexScanner getFieldIndexScanner() {
        return new FieldIndexScanner(client, auths, tableName, "scanId", preNormalizedAttributeFactory);
    }

    private EventScanner getEventScanner() {
        EventScanner scanner;
        if (options.isConfiguredDocumentScan()) {
            scanner = new ConfiguredEventScanner(tableName, auths, client, attributeFactory);
            ((ConfiguredEventScanner) scanner).setIncludeFields(options.getIncludeFields());
            ((ConfiguredEventScanner) scanner).setExcludeFields(options.getExcludeFields());
            ((ConfiguredEventScanner) scanner).setTypeMetadata(options.getTypeMetadata());
            return scanner;
        } else {
            scanner = new DefaultEventScanner(tableName, auths, client, attributeFactory);
            ((DefaultEventScanner) scanner).setIncludeFields(options.getIncludeFields());
            ((DefaultEventScanner) scanner).setExcludeFields(options.getExcludeFields());

        }
        scanner.setLogStats(options.isLogStageSummaryStats());
        return scanner;
    }

    /**
     * Gets a TermFrequencyScanner implementation based on the configured executor options
     *
     * @return a {@link TermFrequencyScanner}
     */
    private TermFrequencyScanner getTfScanner() {
        TermFrequencyScanner scanner;
        if (options.isTfConfiguredScan()) {
            scanner = new TermFrequencyConfiguredScanner(client, auths, tableName, "scanId");
            if (options.isTfSeekingConfiguredScan()) {
                ((TermFrequencyConfiguredScanner) scanner).setSeekingScan(true);
            }
        } else {
            scanner = new TermFrequencySequentialScanner(client, auths, tableName, "scanId");
        }
        scanner.setTermFrequencyFields(options.getTermFrequencyFields());
        scanner.setLogStats(options.isLogStageSummaryStats());
        return scanner;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();

        // 1. find document keys
        Set<String> uids = getDocumentUids(options.getScript());
        uidQueue.addAll(uids);

        // 2. Fire up event aggregation thread
        long aggregationStart = System.currentTimeMillis();
        AtomicBoolean scanningEvents = new AtomicBoolean(true);
        List<Future<?>> futures = new LinkedList<>();
        for (int i = 0; i < numEventThreads; i++) {
            EventAggregationThread eventThread = new EventAggregationThread(uidQueue, eventQueue, eventScanner, options.getRange());
            ListenableFuture<EventAggregationThread> future = (ListenableFuture<EventAggregationThread>) eventThreadPool.submit(eventThread);
            Futures.addCallback(future, eventThread, eventThreadPool);
            futures.add(future);
        }

        FutureCompleteThread completeThread = new FutureCompleteThread(scanningEvents, futures);
        ListenableFuture<FutureCompleteThread> future = (ListenableFuture<FutureCompleteThread>) workerThreads.submit(completeThread);
        Futures.addCallback(future, completeThread, workerThreads);

        AtomicBoolean enrichingEvents = new AtomicBoolean(true);
        documentThreadPool.execute(new EnrichmentThread(eventQueue, candidateQueue, fiScanner, tfScanner, shard, indexOnlyFields, tfFields, terms,
                        options.getScript(), scanningEvents, enrichingEvents, aggregationStart, options.isLogStageSummaryStats()));

        // setup an evaluation thread
        AtomicBoolean evaluating = new AtomicBoolean(true);
        for (int i = 0; i < numEvaluationThreads; i++) {
            EvaluationThread evaluationThread = new EvaluationThread(options.getQuery(), evaluating, enrichingEvents, candidateQueue, documentQueue, stats);
            ListenableFuture<EvaluationThread> f = (ListenableFuture<EvaluationThread>) workerThreads.submit(evaluationThread);
            Futures.addCallback(f, evaluationThread, workerThreads);
        }

        // 4. apply post evaluation transforms such as reduceToKeep, grouping, unique, etc
        // ShardQueryLogic functions in order
        // 0. need to apply time filter using the start/end time to keys coming back
        // 1. map document
        // 2. apply configured post-processing chain
        // 3. masked value filter
        // 4. attribute filter (reduce to keep)
        // 5. apply projections (includes/excludes)
        // 6. apply composite projections
        // 7. empty document filter
        // 8. document metadata (invalidates doc metadata and updates)
        // 9. apply limit fields
        // 10. remove grouping context if it was added by query iterator

        long postProcessingTime = System.currentTimeMillis();
        int returned = 0;
        while (enrichingEvents.get() || evaluating.get() || !documentQueue.isEmpty()) {
            try {
                // TODO
                // 1. increase timeout, observe test pause
                // 2. swap queue for linked blocking queue, observer no pause
                Entry<Key,Document> entry = documentQueue.poll(250, TimeUnit.MICROSECONDS);
                if (entry == null) {
                    continue;
                }
                returned++;
                entry.setValue((Document) entry.getValue().reduceToKeep());

                if (options.getLimitFields() != null) {
                    entry = options.getLimitFields().apply(entry);
                }

                Map.Entry<Key,Value> result = serializer.apply(new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()));
                offerResult(result);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (options.isLogStageSummaryStats() || options.isLogShardSummaryStats()) {
            long stop = System.currentTimeMillis();
            postProcessingTime = stop - postProcessingTime;
            long duration = stop - start;

            if (options.isLogStageSummaryStats()) {
                log.info("time to post-process documents was: " + postProcessingTime + " ms");
                log.info("setup time: " + setupTime);
            }

            if (options.isLogShardSummaryStats()) {
                log.info("query executor for " + shard + " executed in " + duration + " ms, docs evaluated: " + uids.size() + ", docs returned " + returned);
            }
        }

        if (stats != null) {
            stats.incrementShardsSearched();
            stats.logMinimizedStats(log);
            if (globalStats != null) {
                globalStats.merge(stats);
            }
        }
    }

    /**
     * Extracted this method because sonar lint didn't like nested try-catch blocks. Seems reasonable.
     *
     * @param result
     *            a match for the query
     */
    private void offerResult(Map.Entry<Key,Value> result) {
        boolean accepted = false;
        while (!accepted) {
            try {
                accepted = resultQueue.offer(result, 250, TimeUnit.MICROSECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        if (stats != null) {
            stats.incrementDocumentsReturned();
        }
    }

    private Set<String> getDocumentUids(ASTJexlScript script) {
        UidScanner uidScanner = getFiScanner();
        return uidScanner.scan(script, shard, options.getIndexedFields());
    }

    private UidScanner getFiScanner() {
        UidScanner scanner;
        if (options.isUidParallelScan()) {
            scanner = new ParallelUidScanner(uidThreadPool, client, auths, tableName, "scanId");
        } else {
            scanner = new SerialUidScanner(client, auths, tableName, "scanId");
        }
        scanner.setLogStats(options.isLogStageSummaryStats());

        if (options.getStartTime() != -1 && options.getEndTime() != -1) {
            TimeFilter timeFilter = new TimeFilter(options.getStartTime(), options.getEndTime());
            scanner.withTimeFilter(timeFilter);
        }

        if (options.getDatatypeFilter() != null && !options.getDatatypeFilter().isEmpty()) {
            scanner.withDatatypeFilter(options.getDatatypeFilter());
        }

        return scanner;
    }

    public ScanStats getStats() {
        return stats;
    }
}
