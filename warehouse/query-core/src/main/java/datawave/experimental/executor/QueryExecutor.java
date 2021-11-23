package datawave.experimental.executor;

import java.util.AbstractMap;
import java.util.HashSet;
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
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import datawave.experimental.QueryTermVisitor;
import datawave.experimental.fi.ParallelUidScanner;
import datawave.experimental.fi.SerialUidScanner;
import datawave.experimental.fi.UidScanner;
import datawave.experimental.scanner.EventScanner;
import datawave.experimental.scanner.FieldIndexScanner;
import datawave.experimental.scanner.tf.TermFrequencyConfiguredScanner;
import datawave.experimental.scanner.tf.TermFrequencyScanner;
import datawave.experimental.scanner.tf.TermFrequencySequentialScanner;
import datawave.experimental.threads.EvaluationThread;
import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttributeFactory;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.jexl.DatawaveJexlContext;
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

    Set<JexlNode> terms;

    private final Set<String> nonEventFields = new HashSet<>();

    private final PreNormalizedAttributeFactory preNormalizedAttributeFactory;
    private final AttributeFactory attributeFactory;

    private final LinkedBlockingQueue<Tuple3<Key,Document,DatawaveJexlContext>> candidateQueue;
    private final LinkedBlockingQueue<Entry<Key,Document>> documentQueue;
    private final BlockingQueue<Entry<Key,Value>> resultQueue;

    private final ExecutorService uidThreadPool;
    private final ExecutorService documentThreadPool;

    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();

    private final FieldIndexScanner fiScanner;
    private final EventScanner eventScanner;
    private final TermFrequencyScanner tfScanner;

    private final String shard;

    private long setupTime;

    private final int numEvaluationThreads = 1;
    private final ExecutorService evaluationThreadPool = Executors.newFixedThreadPool(numEvaluationThreads);

    public QueryExecutor(QueryExecutorOptions options, MetadataHelper metadataHelper, BlockingQueue<Entry<Key,Value>> results, ExecutorService uidThreadPool,
                    ExecutorService documentThreadPool) {
        setupTime = System.currentTimeMillis();

        this.options = options;
        this.client = options.getClient();
        this.tableName = options.getTableName();
        this.auths = options.getAuths();
        this.metadataHelper = metadataHelper;

        this.preNormalizedAttributeFactory = new PreNormalizedAttributeFactory(options.getTypeMetadata());
        this.attributeFactory = new AttributeFactory(options.getTypeMetadata());

        this.candidateQueue = new LinkedBlockingQueue<>(50);
        this.documentQueue = new LinkedBlockingQueue<>(50);
        this.resultQueue = results;

        this.uidThreadPool = uidThreadPool;
        this.documentThreadPool = documentThreadPool;

        this.terms = QueryTermVisitor.parse(options.getScript());

        this.fiScanner = getFieldIndexScanner();
        this.eventScanner = getEventScanner();
        this.tfScanner = getTfScanner();

        this.shard = options.getRange().getStartKey().getRow().toString();
        setupTime = System.currentTimeMillis() - setupTime;
    }

    private FieldIndexScanner getFieldIndexScanner() {
        return new FieldIndexScanner(client, auths, tableName, "scanId", preNormalizedAttributeFactory);
    }

    private EventScanner getEventScanner() {
        return new EventScanner(tableName, auths, client, attributeFactory);
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
        return scanner;
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        log.info("query executor start");

        // 1. find document keys
        Set<String> uids = getDocumentUids(options.getScript());
        log.info("found " + uids.size() + " uids");

        // 2. aggregate documents
        long aggregationStart = System.currentTimeMillis();
        AtomicBoolean aggregating = new AtomicBoolean(true);
        List<Future<?>> futures = new LinkedList<>();
        for (String uid : uids) {
            Future<?> future = documentThreadPool.submit(() -> {
                Tuple3<Key,Document,DatawaveJexlContext> tuple = fetchDocument(uid);
                boolean accepted = false;
                while (!accepted) {
                    try {
                        accepted = candidateQueue.offer(tuple, 1, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            futures.add(future);
        }

        // this thread provides wall clock timing for how long it takes to aggregate all documents
        AggregationThread aggregationThread = new AggregationThread(aggregating, futures, aggregationStart, shard);
        uidThreadPool.execute(aggregationThread);

        // setup an evaluation thread
        AtomicBoolean evaluating = new AtomicBoolean(true);
        for (int i = 0; i < numEvaluationThreads; i++) {
            evaluationThreadPool.submit(new EvaluationThread(options.getQuery(), evaluating, aggregating, candidateQueue, documentQueue));
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
        while (aggregating.get() || evaluating.get() || !documentQueue.isEmpty()) {
            try {
                // TODO
                // 1. increase timeout, observe test pause
                // 2. swap queue for linked blocking queue, observer no pause
                Entry<Key,Document> entry = documentQueue.poll(1, TimeUnit.MILLISECONDS);
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
        postProcessingTime = System.currentTimeMillis() - postProcessingTime;
        log.info("time to post-process documents was: " + postProcessingTime + " ms");

        long duration = System.currentTimeMillis() - start;
        log.info("setup time: " + setupTime);
        log.info("query executor for " + shard + " executed in " + duration + " ms, docs evaluated: " + uids.size() + ", docs returned " + returned);
        // TODO -- log stats here
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
                accepted = resultQueue.offer(result, 1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
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
        return scanner;
    }

    private Tuple3<Key,Document,DatawaveJexlContext> fetchDocument(String dtUid) {
        long start = System.currentTimeMillis();
        // 1. Simply fetch the document.
        Document d = eventScanner.fetchDocument(options.getRange(), dtUid);

        // 2. Need to check for index only fields
        Set<String> indexOnlyFields = checkQueryForIndexOnlyFields();
        if (!indexOnlyFields.isEmpty()) {
            // fetch index only fields from the FieldIndex
            fiScanner.fetchIndexOnlyFields(d, shard, dtUid, indexOnlyFields, terms);
        }

        DatawaveJexlContext context = new DatawaveJexlContext();

        // 3. Fetch any term frequency fields, if they exist
        Set<String> tfFields = checkQueryForTermFrequencyFields();
        if (!tfFields.isEmpty()) {
            tfScanner.setTermFrequencyFields(tfFields);
            Map<String,Object> offsets = tfScanner.fetchOffsets(options.getScript(), d, shard, dtUid);
            d.visit(new HashSet<>(), context);
            context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, offsets.get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME));
        } else {
            d.visit(new HashSet<>(), context);
        }

        long elapsed = System.currentTimeMillis() - start;
        log.info("time to aggregate single document " + dtUid + " was " + elapsed + " ms");
        return new Tuple3<>(new Key(options.getRange().getStartKey().getRow().toString(), dtUid), d, context);
    }

    private Set<String> checkQueryForIndexOnlyFields() {
        Set<String> queryIndexOnlyFields = new HashSet<>();
        for (String indexOnlyField : options.getIndexOnlyFields()) {
            if (options.getQuery().contains(indexOnlyField)) {
                queryIndexOnlyFields.add(indexOnlyField);
            }
        }

        return queryIndexOnlyFields;
    }

    private Set<String> checkQueryForTermFrequencyFields() {
        Set<String> queryTermFrequencyFields = new HashSet<>();
        for (String termFrequencyField : options.getTermFrequencyFields()) {
            if (options.getQuery().contains(termFrequencyField)) {
                queryTermFrequencyFields.add(termFrequencyField);
            }
        }
        return queryTermFrequencyFields;
    }

    /**
     * Simple threads that watches a list a futures and updates a boolean when all futures are complete
     */
    public static class AggregationThread implements Runnable {

        private final AtomicBoolean aggregating;
        private final List<Future<?>> futures;
        private final long start;
        private final String shard;
        private final int documentCount;

        public AggregationThread(AtomicBoolean aggregating, List<Future<?>> futures, long start, String shard) {
            this.aggregating = aggregating;
            this.futures = futures;
            this.start = start;
            this.shard = shard;
            this.documentCount = futures.size();
        }

        @Override
        public void run() {
            List<Future<?>> completed = new LinkedList<>();
            while (!futures.isEmpty()) {
                for (Future<?> future : futures) {
                    if (future.isDone() || future.isCancelled()) {
                        completed.add(future);
                    }
                }
                futures.removeAll(completed);
                completed.clear();
            }
            aggregating.set(false);
            long elapsed = System.currentTimeMillis() - start;
            if (documentCount > 0) {
                log.info("time to aggregate all documents for shard " + shard + ": " + documentCount + " in " + elapsed + " ms (" + (elapsed / documentCount)
                                + " ms per doc)");
            }
        }
    }
}
