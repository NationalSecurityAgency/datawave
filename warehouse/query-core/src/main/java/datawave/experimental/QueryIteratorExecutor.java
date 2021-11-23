package datawave.experimental;

import com.google.common.collect.Maps;
import datawave.experimental.doc.DocumentFetcher;
import datawave.experimental.fi.FiScanner;
import datawave.experimental.fi.FiScannerStrategy;
import datawave.experimental.fi.ParallelFiScanner;
import datawave.experimental.intersect.UidIntersection;
import datawave.experimental.intersect.UidIntersectionStrategy;
import datawave.experimental.util.ScanStats;
import datawave.experimental.util.ScannerChunkUtil;
import datawave.query.Constants;
import datawave.query.attributes.Document;
import datawave.query.composite.CompositeMetadata;
import datawave.query.function.Aggregation;
import datawave.query.function.JexlEvaluation;
import datawave.query.iterator.aggregation.DocumentData;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.HitListArithmetic;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.ValidComparisonVisitor;
import datawave.query.predicate.TimeFilter;
import datawave.query.tables.async.ScannerChunk;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.query.util.MetadataHelper;
import datawave.query.util.Tuple3;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static datawave.experimental.util.ScannerChunkUtil.*;
import static datawave.experimental.util.ScannerChunkUtil.indexOnlyFieldsFromChunk;
import static datawave.experimental.util.ScannerChunkUtil.indexedFieldsFromChunk;
import static datawave.experimental.util.ScannerChunkUtil.queryFromChunk;
import static datawave.experimental.util.ScannerChunkUtil.rangeFromChunk;
import static datawave.experimental.util.ScannerChunkUtil.scanIdFromChunk;
import static datawave.experimental.util.ScannerChunkUtil.termFrequencyFieldsFromChunk;
import static datawave.experimental.util.ScannerChunkUtil.tfRequiredFromChunk;

/**
 * Functionally similar to the {@link datawave.query.iterator.QueryIterator} except document evaluation happens outside of the tablet server.
 * <p>
 * Handles evaluating shard and document ranges against a particular tablet.
 * <p>
 * Separate thread pools may be set to handle field index lookups and document evaluation in parallel.
 * <p>
 * All the same operations are implemented here
 *
 * <pre>
 *     - shard range queries
 *     - document range queries
 *     - index only terms vs. event only terms
 *     - term frequency enrichment
 * </pre>
 * <p>
 * TODO
 *
 * <pre>
 *     1. Field Index satisfies query (evaluate after FI scan)
 *     2. Return fields specified (filter fields being aggregated. could be a specialized iterator).
 *     3. unique function.
 * </pre>
 */
public class QueryIteratorExecutor implements Callable<QueryIteratorExecutor> {
    
    private static final Logger log = Logger.getLogger(QueryIteratorExecutor.class);
    
    // flag that signals when this callable is done working
    AtomicBoolean working = new AtomicBoolean(false);
    AtomicInteger docsToEvaluate = new AtomicInteger(0);
    
    // support for parallel document evaluation
    AtomicBoolean isSubmitting = new AtomicBoolean(false);
    AtomicBoolean isExecuting = new AtomicBoolean(false);
    // Executor submitThread;
    Executor resultThread; // only used by parallel. can collapse these two
    Executor executor; // only used by sequential evaluation
    
    private final ExecutorService fieldIndexPool;
    private final ExecutorService evaluationPool;
    
    // scan id is the queryId::range.startKey(optionally ::uid)
    private String scanId;
    
    String query;
    ASTJexlScript script;
    Set<JexlNode> terms;
    Map<String,Set<String>> nodesToUids;
    
    Connector conn;
    String tableName;
    Authorizations auths;
    MetadataHelper metadataHelper;
    
    FiScannerStrategy fiScanner;
    UidIntersectionStrategy uidIntersect;
    DocumentFetcher docFetcher;
    
    // some internal stats
    private ScanStats scanStats = new ScanStats();
    
    public enum STATE {
        INIT, FIELD_INDEX, DOC_EVAL, DONE
    }
    
    // scanner chunk specific datas
    private Range range;
    private String shard;
    
    private Set<String> indexedFields;
    private Set<String> indexOnlyFields;
    private Set<String> termFrequencyFields;
    private boolean tfRequired;
    private boolean sequentialEvaluationOnly = false;
    
    private TimeFilter timeFilter = new TimeFilter(0, Long.MAX_VALUE);
    private TypeMetadata typeMetadata = null;
    private CompositeMetadata compositeMetadata = null;
    private Aggregation aggregation;
    
    private final LinkedBlockingQueue<Map.Entry<Key,Document>> results;
    
    public QueryIteratorExecutor(LinkedBlockingQueue<Map.Entry<Key,Document>> results, ExecutorService fieldIndexPool, ExecutorService evaluationPool,
                    Connector conn, String tableName, Authorizations auths, MetadataHelper metadataHelper) {
        this.results = results;
        this.fieldIndexPool = fieldIndexPool;
        this.evaluationPool = evaluationPool;
        
        this.conn = conn;
        this.tableName = tableName;
        this.auths = auths;
        this.metadataHelper = metadataHelper;
        
        this.uidIntersect = new UidIntersection();
    }
    
    public void setScannerChunk(ScannerChunk chunk) {
        // parse a few things out
        this.range = rangeFromChunk(chunk);
        this.shard = this.range.getStartKey().getRow().toString();
        this.scanId = scanIdFromChunk(chunk);
        this.indexedFields = indexedFieldsFromChunk(chunk);
        this.indexOnlyFields = indexOnlyFieldsFromChunk(chunk);
        this.termFrequencyFields = termFrequencyFieldsFromChunk(chunk);
        this.tfRequired = tfRequiredFromChunk(chunk);
        this.typeMetadata = typeMetadataFromChunk(chunk);
        this.compositeMetadata = compositeMetadataFromChunk(chunk);
        
        if (typeMetadata != null && compositeMetadata != null) {
            boolean includeGroupingContext = false;
            boolean includeRecordId = true;
            boolean disableIndexOnlyDocuments = false;
            boolean trackSizes = true;
            this.aggregation = new Aggregation(timeFilter, typeMetadata, compositeMetadata, includeGroupingContext, includeRecordId, disableIndexOnlyDocuments,
                            null, trackSizes);
        }
        
        // create here after scan id is available
        this.docFetcher = new DocumentFetcher(scanId, conn, tableName, auths);
        
        try {
            this.query = queryFromChunk(chunk);
            this.script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (ParseException e) {
            log.error("Failed to parse and flatten query: " + query);
            log.error("Error was: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    @Override
    public QueryIteratorExecutor call() throws Exception {
        working.getAndSet(true);
        executeForRange(range);
        
        while (working.get()) {
            Thread.sleep(1);
        }
        
        logStats();
        return this;
    }
    
    /**
     * Single entry point, splits into either document specific range or shard range
     *
     * @param range
     *            either a shard or document range
     */
    public void executeForRange(Range range) {
        // determine execution path
        String cf = range.getStartKey().getColumnFamily().toString();
        if (!cf.isEmpty()) {
            // column family is the datatype\0uid
            executeForDocumentRange(cf);
        } else {
            executeForShardRange();
        }
    }
    
    /**
     * Evaluate the document given the specified uid
     *
     * @param uid
     *            a document uid
     */
    public void executeForDocumentRange(String uid) {
        terms = parseTermsFromQuery();
        evaluateDocument(uid, true);
        working.getAndSet(false);
    }
    
    /**
     * Find candidate documents from the field index
     */
    public void executeForShardRange() {
        
        // pick a field index scanning strategy, just in time
        terms = parseTermsFromQuery();
        if (terms.size() < 10) {
            this.fiScanner = new FiScanner(scanId, conn, tableName, auths, metadataHelper);
        } else {
            this.fiScanner = new ParallelFiScanner(fieldIndexPool, scanId, conn, tableName, auths, metadataHelper);
        }
        
        // populate node to uid map
        scanFieldIndexForTerms(shard, terms, indexedFields);
        
        // find candidate documents
        SortedSet<String> uids = intersectUids();
        scanStats.incrementUidsTotal(uids.size());
        
        // evaluate docs serially or in parallel
        evaluateDocuments(uids, false);
    }
    
    private Set<JexlNode> parseTermsFromQuery() {
        return QueryTermVisitor.parse(script);
    }
    
    private void scanFieldIndexForTerms(String shard, Set<JexlNode> terms, Set<String> indexedFields) {
        this.nodesToUids = fiScanner.scanFieldIndexForTerms(shard, terms, indexedFields);
    }
    
    private SortedSet<String> intersectUids() {
        return uidIntersect.intersect(script, nodesToUids);
    }
    
    /**
     * Evaluate documents either serially or in parallel
     *
     * @param uids
     *            the set of document ids to evaluate
     * @param isDocRange
     *            true
     */
    private void evaluateDocuments(SortedSet<String> uids, boolean isDocRange) {
        if (uids.size() <= 3 || sequentialEvaluationOnly) {
            log.info("evaluating " + uids.size() + " docs sequentially");
            sequentialEvaluation(uids, isDocRange);
        } else {
            log.info("evaluating " + uids.size() + " docs in parallel");
            parallelEvaluation(uids, isDocRange);
        }
    }
    
    /**
     * Evaluate all documents serially
     *
     * @param uids
     *            the set of documents to evaluate
     * @param isDocRange
     *            TODO -- remove
     */
    private void sequentialEvaluation(SortedSet<String> uids, boolean isDocRange) {
        long start = System.currentTimeMillis();
        isExecuting.getAndSet(true);
        executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            Thread.currentThread().setName(scanId);
            try {
                for (String uid : uids) {
                    log.info("evaluating doc: " + uid);
                    evaluateDocument(uid, isDocRange);
                }
            } catch (Exception e) {
                log.error("error while evaluating documents sequentially: " + e.getMessage());
                e.printStackTrace();
            } finally {
                isExecuting.getAndSet(false);
                log.info("time to evaluate " + uids.size() + " documents sequentially was " + (System.currentTimeMillis() - start) + " ms");
                
                // signal that this callable is done
                working.getAndSet(false);
            }
        });
    }
    
    /**
     * Evaluate documents in parallel
     *
     * @param uids
     *            the set of documents to evaluate
     * @param isDocRange
     *            TODO -- remove
     */
    private void parallelEvaluation(SortedSet<String> uids, boolean isDocRange) {
        long start = System.currentTimeMillis();
        isSubmitting.getAndSet(true);
        isExecuting.getAndSet(true);
        docsToEvaluate.getAndSet(uids.size());
        
        // this thread waits for the evaluation pool to be done
        resultThread = Executors.newSingleThreadExecutor();
        resultThread.execute(() -> {
            Thread.currentThread().setName(scanId);
            while (isSubmitting.get() || docsToEvaluate.get() > 0) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            isExecuting.getAndSet(false);
            log.info("time to evaluate " + uids.size() + " documents in parallel was " + (System.currentTimeMillis() - start) + " ms");
            working.getAndSet(false);
        });
        
        for (String uid : uids) {
            evaluationPool.submit(() -> {
                log.trace("evaluating doc: " + uid);
                evaluateDocument(uid, isDocRange);
                log.trace("done evaluating doc");
                docsToEvaluate.getAndDecrement();
            });
        }
        isSubmitting.getAndSet(false);
    }
    
    /**
     * Fetches the document for the specified uid and evaluates it against the query. Matches are added to the results list.
     *
     * @param dtUid
     *            the datatype and document uid
     * @param isDocRange
     */
    private boolean evaluateDocument(String dtUid, boolean isDocRange) {
        long start = System.currentTimeMillis();
        DatawaveJexlContext context = new DatawaveJexlContext();
        JexlEvaluation evaluation = new JexlEvaluation(query, new HitListArithmetic());
        
        // 1. Build the range
        Range range = buildDocumentRange(dtUid);
        String row = range.getStartKey().getRow().toString();
        
        // 2. Fetch document and populate the context
        Document doc = fetchDocument(range, dtUid, context, isDocRange);
        // List<Map.Entry<Key,Value>> attrs = docFetcher.fetchDocumentAttributes(range, dtUid);
        
        // uid is actually the datatype\0uid
        Key docKey = new Key(row, dtUid);
        
        // Document d = null;
        // if (aggregation != null) {
        // DocumentData docData = new DocumentData(docKey, Collections.singleton(docKey), attrs);
        // d = aggregation.apply(new AbstractMap.SimpleEntry<>(docData, new Document())).getValue();
        // } else {
        // d = fetchDocument(range, dtUid, context, isDocRange);
        // }
        
        // d.visit(new HashSet<>(), context);
        
        if (aggregation != null) {
            throw new IllegalStateException("We don't support aggregation at this time");
        }
        
        Tuple3<Key,Document,DatawaveJexlContext> tuple = new Tuple3<>(docKey, doc, context);
        boolean result = evaluation.apply(tuple);
        // log.info("result: " + result);
        scanStats.incrementDocumentsEvaluated();
        if (result) {
            // offer until accepted
            try {
                Map.Entry<Key,Document> entry = new AbstractMap.SimpleEntry<>(tuple.first(), tuple.second());
                // log.info("fetched doc: " + entry.toString());
                boolean accepted = false;
                while (!accepted) {
                    accepted = results.offer(entry, 10, TimeUnit.MILLISECONDS);
                    // log.info("accepted: " + accepted);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            scanStats.incrementDocumentsReturned();
        }
        log.info("time to evaluate document " + dtUid + " was " + (System.currentTimeMillis() - start) + " ms");
        return result;
    }
    
    /**
     *
     * @param uid
     *            is datatype\0uid
     * @return
     */
    private Range buildDocumentRange(String uid) {
        boolean isTld = false;
        Key start = new Key(shard, uid);
        Key end;
        if (isTld) {
            end = new Key(shard, uid + Constants.MAX_UNICODE_STRING);
        } else {
            end = start.followingKey(PartialKey.ROW_COLFAM);
        }
        return new Range(start, true, end, false);
    }
    
    private Document fetchDocument(Range range, String uid, DatawaveJexlContext context, boolean isDocRange) {
        
        // 1. Simply fetch the document.
        Document d = docFetcher.fetchDocument(range, uid);
        
        // 2. Need to check for index only fields
        Set<String> indexOnlyFields = checkQueryForIndexOnlyFields();
        if (!indexOnlyFields.isEmpty()) {
            if (isDocRange) {
                // fetch index only fields from the FieldIndex
                String shard = range.getStartKey().getRow().toString();
                docFetcher.fetchIndexOnlyFields(d, shard, uid, indexOnlyFields, terms);
            } else {
                // create index only fields from the already fetched uids
                docFetcher.createIndexOnlyFields(d, shard, indexOnlyFields, uid, nodesToUids);
            }
        }
        
        // 3. Fetch any term frequency fields, if they exist
        Set<String> tfFields = checkQueryForTermFrequencyFields();
        if (!tfFields.isEmpty()) {
            String shard = range.getStartKey().getRow().toString();
            Map<String,Object> offsets = docFetcher.fetchTermFrequencyOffsets(script, d, shard, uid, tfFields);
            if (context != null) {
                d.visit(new HashSet<>(), context);
                context.set(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME, offsets.get(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME));
            }
        } else {
            d.visit(new HashSet<>(), context);
        }
        
        return d;
    }
    
    /**
     * Stub for now. Should use the Metadata helper
     *
     * @return the set of index only fields that exist in the query, or an empty set if no such fields are present
     */
    private Set<String> checkQueryForIndexOnlyFields() {
        Set<String> queryIndexOnlyFields = new HashSet<>();
        for (String indexOnlyField : indexOnlyFields) {
            if (query.contains(indexOnlyField)) {
                queryIndexOnlyFields.add(indexOnlyField);
            }
        }
        return queryIndexOnlyFields;
    }
    
    /**
     * Stub. For now just return any index only fields
     *
     * @return
     */
    private Set<String> checkQueryForTermFrequencyFields() {
        Set<String> queryIndexOnlyFields = new HashSet<>();
        for (String indexOnlyField : termFrequencyFields) {
            if (query.contains(indexOnlyField)) {
                queryIndexOnlyFields.add(indexOnlyField);
            }
        }
        return queryIndexOnlyFields;
    }
    
    /**
     * Log some basic stats
     */
    public void logStats() {
        if (docFetcher != null) {
            this.scanStats = scanStats.merge(docFetcher.getScanStats());
        }
        log.info("---------- " + scanId + " ----------");
        log.info(scanStats.toString());
    }
    
    public boolean isExecuting() {
        return isExecuting.get();
    }
}
