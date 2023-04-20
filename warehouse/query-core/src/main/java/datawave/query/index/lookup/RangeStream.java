package datawave.query.index.lookup;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.query.CloseableIterable;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.index.lookup.IndexStream.StreamContext;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.DepthVisitor;
import datawave.query.jexl.visitors.EvaluationRendering;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.RangeStreamScanner;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.SessionOptions;
import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryScannerHelper;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;
import datawave.util.StringUtils;
import datawave.util.time.DateHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnknownFieldERNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;

public class RangeStream extends BaseVisitor implements CloseableIterable<QueryPlan> {

    private static final int MAX_MEDIAN = 20;

    private static final Logger log = ThreadConfigurableLogger.getLogger(RangeStream.class);

    /**
     * An assignment to this variable can be used to specify a stream of shards and days anywhere in the query. Used by the date function index query creation.
     */

    protected final ShardQueryConfiguration config;
    protected final ScannerFactory scanners;
    protected final MetadataHelper metadataHelper;
    protected Iterator<QueryPlan> itr;
    protected StreamContext context;
    protected BaseIndexStream queryStream;
    protected boolean limitScanners = false;
    protected Class<? extends SortedKeyValueIterator<Key, Value>> createUidsIteratorClass = CreateUidsIterator.class;
    protected Multimap<String, Type<?>> fieldDataTypes;

    protected BlockingQueue<Runnable> runnables;

    protected JexlNode tree = null;

    protected UidIntersector uidIntersector = new IndexInfo();

    /**
     * Intended to reduce the cost of repeated calls to helper.getAllFields
     */
    protected Set<String> helperAllFieldsCache = new HashSet<>();

    private int maxScannerBatchSize;

    protected ExecutorService executor;

    protected ExecutorService streamExecutor;

    protected boolean collapseUids = false;

    protected Set<String> indexOnlyFields = Sets.newHashSet();

    public RangeStream(ShardQueryConfiguration config, ScannerFactory scanners, MetadataHelper metadataHelper) {
        this.config = config;
        this.scanners = scanners;
        this.metadataHelper = metadataHelper;
        int maxLookup = (int) Math.max(config.getNumIndexLookupThreads(), 1);
        executor = Executors.newFixedThreadPool(maxLookup);
        runnables = new LinkedBlockingDeque<>();
        int executeLookupMin = Math.max(maxLookup / 2, 1);
        streamExecutor = new ThreadPoolExecutor(executeLookupMin, maxLookup, 100, TimeUnit.MILLISECONDS, runnables);
        fieldDataTypes = config.getQueryFieldsDatatypes();
        collapseUids = config.getCollapseUids();
        try {
            Set<String> ioFields = metadataHelper.getIndexOnlyFields(null);
            if (null != ioFields) {
                indexOnlyFields.addAll(ioFields);
            }
        } catch (TableNotFoundException e) {
            // ignore
        }
    }

    public CloseableIterable<QueryPlan> streamPlans(JexlNode script) {
        JexlNode node = TreeFlatteningRebuildingVisitor.flatten(script);

        tree = node;

        if (!collapseUids && config.getParseTldUids()) {
            collapseUids = !(EvaluationRendering.canDisableEvaluation(script, config, metadataHelper, true));

            if (log.isTraceEnabled()) {
                log.trace("New query is " + JexlStringBuildingVisitor.buildQuery(tree));
                log.trace("Collapse UIDs is now " + collapseUids + " because we have a TLD Query with an ivarator");
            }
        }

        // check the query depth (up to config.getMaxDepthThreshold() + 1)
        int depth = DepthVisitor.getDepth(node, config.getMaxDepthThreshold());
        if (depth > config.getMaxDepthThreshold()) {
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_DEPTH_THRESHOLD_EXCEEDED, MessageFormat.format(
                    "{0} > {1}, last operation: {2}", depth, config.getMaxDepthThreshold(), "RangeStreamLookup"));
            throw new DatawaveFatalQueryException(qe);
        }

        if (log.isTraceEnabled()) {
            log.trace(JexlStringBuildingVisitor.buildQuery(node));
        }

        BaseIndexStream ranges = (BaseIndexStream) node.jjtAccept(this, null);

        // Guards against the case of a very oddly formed JEXL query, e.g. ("foo")
        if (null == ranges) {
            this.context = StreamContext.UNINDEXED;
            this.itr = Collections.emptyIterator();
        } else {
            // we can build the iterator at a later point, grabbing the top most
            // context. This will usually provide us a hint about the context
            // within our stream.
            context = ranges.context();
            this.itr = null;
        }
        if (log.isDebugEnabled()) {
            log.debug("Query returned a stream with a context of " + this.context);
            if (queryStream != null) {
                for (String line : StringUtils.split(queryStream.getContextDebug(), '\n')) {
                    log.debug(line);
                }
            }
        }

        this.queryStream = ranges;

        return this;
    }

    /**
     * Call shutdownNow() on the underlying {@link ExecutorService}
     */
    protected void shutdownThreads() {
        executor.shutdownNow();
    }

    @Override
    public Iterator<QueryPlan> iterator() {
        try {
            if (null == itr) {
                if (queryStream.context() == StreamContext.INITIALIZED) {
                    List<ConcurrentScannerInitializer> todo = Lists.newArrayList();
                    todo.add(new ConcurrentScannerInitializer(queryStream));
                    Collection<BaseIndexStream> streams = ConcurrentScannerInitializer.initializeScannerStreams(todo, executor);
                    if (streams.size() == 1) {
                        queryStream = streams.iterator().next();
                    }
                }
                if (queryStream.context() == StreamContext.VARIABLE) {
                    if (queryStream instanceof Union) {
                        context = StreamContext.ABSENT;
                        queryStream = null;
                        itr = Collections.emptyIterator();
                        return itr;
                    } else {
                        context = StreamContext.PRESENT;
                    }
                } else {
                    context = queryStream.context();
                }

                if (log.isDebugEnabled()) {
                    log.debug("Query returned a stream with a context of " + this.context);
                    for (String line : StringUtils.split(queryStream.getContextDebug(), '\n')) {
                        log.debug(line);
                    }
                }

                this.itr = filter(concat(transform(queryStream, new TupleToRange(queryStream.currentNode(), config))), new EmptyPlanPruner());
            }
        } finally {
            // shut down the executor as all threads have completed
            shutdownThreads();
        }
        return itr;
    }

    public static class EmptyPlanPruner implements Predicate<QueryPlan> {

        public boolean apply(QueryPlan plan) {
            if (log.isTraceEnabled()) {
                if (null != plan.getQueryTree() || (null == plan.getQueryString() || plan.getQueryString().isEmpty())) {
                    log.trace("Plan is " + JexlStringBuildingVisitor.buildQuery(plan.getQueryTree()) + " " + plan.getRanges() + " "
                            + plan.getRanges().iterator().hasNext());
                } else {
                    log.trace("Plan is " + plan.getQueryTree() + " " + plan.getRanges() + " " + plan.getRanges().iterator().hasNext());
                }
            }
            return plan.getRanges().iterator().hasNext();
        }
    }

    public static class MinimizeRanges implements Function<QueryPlan, QueryPlan> {

        StreamContext myContext;

        public MinimizeRanges(StreamContext myContext) {
            this.myContext = myContext;
        }

        public QueryPlan apply(QueryPlan plan) {

            if (StreamContext.EXCEEDED_TERM_THRESHOLD == myContext || StreamContext.EXCEEDED_VALUE_THRESHOLD == myContext) {

                Set<Range> newRanges = Sets.newHashSet();

                for (Range range : plan.getRanges()) {
                    if (isEventSpecific(range)) {
                        Key topKey = range.getStartKey();
                        newRanges.add(new Range(topKey.getRow().toString(), true, topKey.getRow() + Constants.NULL_BYTE_STRING, false));
                    } else {
                        newRanges.add(range);
                    }
                }
                plan.setRanges(newRanges);
            }
            return plan;
        }
    }

    public StreamContext context() {
        return context;
    }

    @Override
    public IndexStream visit(ASTOrNode node, Object data) {
        Union.Builder builder = Union.builder();
        List<ConcurrentScannerInitializer> todo = Lists.newArrayList();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            BaseIndexStream child = (BaseIndexStream) node.jjtGetChild(i).jjtAccept(this, builder);
            if (null != child) {
                todo.add(new ConcurrentScannerInitializer(child));
            }
        }

        builder.addChildren(todo);

        if (data instanceof Union.Builder) {
            log.debug("[ASTOrNode] Propagating children up to parent because nodes of the same type.");
            Union.Builder parent = (Union.Builder) data;
            parent.consume(builder);
            return ScannerStream.noOp(node);

        } else if (builder.size() == 0) {
            return ScannerStream.unindexed(node);
        } else {

            Union union = builder.build(executor);

            switch (union.context()) {
                case ABSENT:
                    return ScannerStream.noData(union.currentNode(), union);
                case IGNORED:
                    return ScannerStream.ignored(union.currentNode(), union);
                case DELAYED_FIELD:
                    return ScannerStream.delayedExpression(union.currentNode());
                case PRESENT:
                case VARIABLE:
                case EXCEEDED_TERM_THRESHOLD:
                case EXCEEDED_VALUE_THRESHOLD:
                    return union;
                case UNINDEXED:
                    return ScannerStream.unindexed(union.currentNode(), union);
                case UNKNOWN_FIELD:
                case INITIALIZED:
                    return ScannerStream.unknownField(union.currentNode(), union);
                default:
                    return ScannerStream.unknownField(node, union);
            }
        }
    }

    @Override
    public IndexStream visit(ASTAndNode node, Object data) {
        Intersection.Builder builder = Intersection.builder();
        builder.setUidIntersector(uidIntersector);

        // join the index streams
        List<ConcurrentScannerInitializer> todo = Lists.newArrayList();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            BaseIndexStream child = (BaseIndexStream) node.jjtGetChild(i).jjtAccept(this, builder);
            if (null != child) {
                todo.add(new ConcurrentScannerInitializer(child));
            }
        }

        builder.addChildren(todo);

        if (data instanceof Intersection.Builder) {
            log.debug("[ASTAndNode] Propagating children up to parent because nodes of the same type.");
            Intersection.Builder parent = (Intersection.Builder) data;
            parent.consume(builder);

            return ScannerStream.noOp(node);

        } else if (builder.size() == 0) {
            return ScannerStream.unindexed(node);
        } else {
            Intersection build = builder.build(executor);
            switch (build.context()) {
                case ABSENT:
                    return ScannerStream.noData(build.currentNode(), build);
                case IGNORED:
                    return ScannerStream.ignored(build.currentNode(), build);
                case EXCEEDED_TERM_THRESHOLD:
                case EXCEEDED_VALUE_THRESHOLD:
                case PRESENT:
                case VARIABLE:
                    return build;
                case UNINDEXED:
                    return ScannerStream.unindexed(build.currentNode(), build);
                case UNKNOWN_FIELD:
                case INITIALIZED:
                    return ScannerStream.unknownField(build.currentNode(), build);
                default:
                    return ScannerStream.unknownField(node, build);
            }
        }
    }

    @Override
    public ScannerStream visit(ASTEQNode node, Object data) {

        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        // We are looking for identifier = literal
        IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op == null) {
            return ScannerStream.unindexed(node);
        }

        final String fieldName = op.deconstructIdentifier();

        // Null literals cannot be resolved against the index.
        if (op.getLiteralValue() == null) {
            return ScannerStream.unindexed(node);
        }

        // toString of String returns the String
        String literal = op.getLiteralValue().toString();

        if (QueryOptions.DEFAULT_DATATYPE_FIELDNAME.equals(fieldName)) {
            return ScannerStream.unindexed(node);
        }

        // Check if field is not indexed
        if (!isIndexed(fieldName, config.getIndexedFields())) {
            try {
                if (this.getAllFieldsFromHelper().contains(fieldName)) {
                    log.debug("{\"" + fieldName + "\": \"" + literal + "\"} is not indexed.");
                    return ScannerStream.unindexed(node);
                }
            } catch (TableNotFoundException | ExecutionException | MarkingFunctions.Exception e) {
                log.error(e);
                throw new RuntimeException(e);
            }
            log.debug("{\"" + fieldName + "\": \"" + literal + "\"} is not an observed field.");
            return ScannerStream.unknownField(node);
        }

        // Final case, field is indexed
        log.debug("\"" + fieldName + "\" is indexed. for " + literal);
        try {

            int stackStart = config.getBaseIteratorPriority();

            RangeStreamScanner scannerSession;

            SessionOptions options = new SessionOptions();
            options.fetchColumnFamily(new Text(fieldName));
            options.addScanIterator(makeDataTypeFilter(config, stackStart++));

            final IteratorSetting uidSetting;

            // Create the range for the term from the provided config.
            Range range = rangeForTerm(literal, fieldName, config);

            if (limitScanners) {
                // Setup the CreateUidsIterator
                scannerSession = scanners.newRangeScanner(config.getIndexTableName(), config.getAuthorizations(), config.getQuery(),
                        config.getShardsPerDayThreshold());

                uidSetting = new IteratorSetting(stackStart++, createUidsIteratorClass);
                uidSetting.addOption(CreateUidsIterator.COLLAPSE_UIDS, Boolean.valueOf(collapseUids).toString());
                uidSetting.addOption(CreateUidsIterator.PARSE_TLD_UIDS, Boolean.valueOf(config.getParseTldUids()).toString());

            } else {
                // Setup so this is a pass-through
                scannerSession = scanners.newRangeScanner(config.getIndexTableName(), config.getAuthorizations(), config.getQuery(),
                        config.getShardsPerDayThreshold());

                uidSetting = new IteratorSetting(stackStart++, createUidsIteratorClass);
                uidSetting.addOption(CreateUidsIterator.COLLAPSE_UIDS, Boolean.valueOf(false).toString());
                uidSetting.addOption(CreateUidsIterator.PARSE_TLD_UIDS, Boolean.valueOf(false).toString());
            }

            /*
             * Create a scanner in the initialized state so that we can scan immediately
             */
            if (log.isTraceEnabled()) {
                log.trace("Building delayed scanner for " + fieldName + ", literal= " + literal);
            }

            // Configure common settings on the ScannerSession
            options.addScanIterator(uidSetting);

            String queryString = fieldName + "=='" + literal + "'";
            options.addScanIterator(QueryScannerHelper.getQueryInfoIterator(config.getQuery(), false, queryString));

            scannerSession.setOptions(options);
            scannerSession.setMaxResults(config.getMaxIndexBatchSize());
            scannerSession.setExecutor(streamExecutor);
            scannerSession.setRanges(Collections.singleton(range));

            // Create the EntryParser prior to ScannerStream.
            EntryParser entryParser = new EntryParser(node, fieldName, literal, indexOnlyFields);

            return ScannerStream.initialized(scannerSession, entryParser, node);

        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    /*
     * Presume that functions have already been expanded with their index query parts @see QueryIndexQueryExpandingVisitor
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        if (log.isTraceEnabled()) {
            log.trace("building delayed expression for function");
        }
        return ScannerStream.delayedExpression(node);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return ScannerStream.delayedExpression(node);
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        if (log.isTraceEnabled()) {
            log.trace("NOT FIELD " + JexlStringBuildingVisitor.buildQuery(node));
        }
        return ScannerStream.delayedExpression(node);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op == null) {
            return ScannerStream.unindexed(node);
        }

        final String fieldName = op.deconstructIdentifier();

        // HACK to make EVENT_DATATYPE queries work
        if (QueryOptions.DEFAULT_DATATYPE_FIELDNAME.equals(fieldName)) {
            return ScannerStream.unindexed(node);
        }

        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.unindexed(node);
        }

        if (node instanceof ASTUnknownFieldERNode) {
            return ScannerStream.unknownField(node);
        }

        return ScannerStream.noData(node);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return ScannerStream.delayedExpression(node);
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return ScannerStream.delayedExpression(node);
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return ScannerStream.noData(node);
    }

    private boolean isUnOrNotFielded(JexlNode node) {
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            if (identifier.image.equals(Constants.ANY_FIELD) || identifier.image.equals(Constants.NO_FIELD)) {
                return true;
            }
        }
        return false;
    }

    private boolean isUnindexed(JexlNode node) {
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            try {
                if (!(identifier.image.equals(Constants.ANY_FIELD) || identifier.image.equals(Constants.NO_FIELD))) {
                    if (!metadataHelper.isIndexed(JexlASTHelper.deconstructIdentifier(identifier), config.getDatatypeFilter())) {
                        return true;
                    }
                }
            } catch (TableNotFoundException | ExecutionException | MarkingFunctions.Exception e) {
                log.error("Could not determine whether field is indexed", e);
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.unindexed(node);
        }

        return ScannerStream.delayedExpression(node);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.unindexed(node);
        }

        return ScannerStream.delayedExpression(node);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.unindexed(node);
        }

        return ScannerStream.delayedExpression(node);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        if (isUnOrNotFielded(node)) {
            return ScannerStream.noData(node);
        }

        if (isUnindexed(node)) {
            return ScannerStream.unindexed(node);
        }

        return ScannerStream.delayedExpression(node);
    }

    public Object descend(JexlNode node, Object data) {
        if (node.jjtGetNumChildren() > 1) {

            QueryException qe = new QueryException(DatawaveErrorCode.MORE_THAN_ONE_CHILD, MessageFormat.format("Class: {0}", node.getClass().getSimpleName()));
            throw new DatawaveFatalQueryException(qe);
        }
        if (node.jjtGetNumChildren() == 1) {
            return node.jjtGetChild(0).jjtAccept(this, data);
        } else {
            return data;
        }
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return descend(node, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        // if we have a term threshold marker, then we simply could not expand an _ANYFIELD_ identifier, so return EXCEEDED_THRESHOLD
        if (instance.isType(ExceededTermThresholdMarkerJexlNode.class)) {
            return ScannerStream.exceededTermThreshold(node);
        } else if (instance.isAnyTypeOf(ExceededValueThresholdMarkerJexlNode.class, ExceededOrThresholdMarkerJexlNode.class)) {
            try {
                // When we exceeded the expansion threshold for a regex, the field is an index-only field, and we can't
                // hook up the hdfs-sorted-set iterator (Ivarator), we can't run the query via the index or
                // full-table-scan, so we throw an Exception
                if (!config.canHandleExceededValueThreshold() && containsIndexOnlyFields(node)) {
                    QueryException qe = new QueryException(DatawaveErrorCode.EXPAND_QUERY_TERM_SYSTEM_LIMITS);
                    throw new DatawaveFatalQueryException(qe);
                }
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.NODE_PROCESSING_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }

            // create a list of tuples for each shard
            if (log.isDebugEnabled()) {
                LiteralRange<?> range = JexlASTHelper.findRange().indexedOnly(config.getDatatypeFilter(), metadataHelper).getRange(node);
                if (range != null) {
                    log.debug("{\"" + range.getFieldName() + "\": \"" + range.getLower() + " - " + range.getUpper() + "\"} requires a full field index scan.");
                } else {
                    log.debug("{\"" + JexlASTHelper.getLiterals(node) + "\"} requires a full field index scan.");
                }
            }
            return ScannerStream.exceededValueThreshold(createFullFieldIndexScanList(config, node).iterator(), node);
        } else if (instance.isAnyTypeOf(ASTDelayedPredicate.class, ASTEvaluationOnly.class)) {
            return ScannerStream.ignored(node);
        } else if (instance.isType(IndexHoleMarkerJexlNode.class)) {
            return ScannerStream.ignored(node);
        } else if (instance.isType(BoundedRange.class)) {
            // here we must have a bounded range that was not expanded, so it must not be expandable via the index
            return ScannerStream.ignored(node);
        } else {
            return descend(node, data);
        }
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return descend(node, data);
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        // If we have an assignment of shards/days, then generate a stream of shards/days
        String identifier = JexlASTHelper.getIdentifier(node);
        if (Constants.SHARD_DAY_HINT.equals(identifier)) {
            JexlNode myNode = JexlNodeFactory.createExpression(node);
            String[] shardsAndDays = StringUtils.split(JexlASTHelper.getLiteralValue(node).toString(), ',');
            if (shardsAndDays.length > 0) {
                return ScannerStream.withData(createIndexScanList(shardsAndDays).iterator(), myNode);
            } else {
                return ScannerStream.noData(myNode);
            }
        }
        return null;
    }

    public Range rangeForTerm(String term, String field, ShardQueryConfiguration config) {
        return rangeForTerm(term, field, config.getBeginDate(), config.getEndDate());
    }

    public Range rangeForTerm(String term, String field, Date start, Date end) {
        return new Range(new Key(term, field, DateHelper.format(start) + "_"), true, new Key(term, field, DateHelper.format(end) + "_" + '\uffff'), false);
    }

    public static IteratorSetting makeDataTypeFilter(ShardQueryConfiguration config, int stackPosition) {
        IteratorSetting is = new IteratorSetting(stackPosition, DataTypeFilter.class);
        is.addOption(DataTypeFilter.TYPES, config.getDatatypeFilterAsString());
        return is;
    }

    public static boolean isIndexed(String field, Multimap<String, Type<?>> ctx) {
        Collection<Type<?>> norms = ctx.get(field);

        return !norms.isEmpty();
    }

    public static boolean isIndexed(String field, Set<String> ctx) {

        return ctx.contains(field);
    }

    public static boolean isNormalized(String field, Set<String> ctx) {
        return ctx.contains(field);
    }

    /**
     * This will create a list of index info (ranges) of the form yyyyMMdd for each day is the specified query date range. Each IndexInfo will have a count of
     * -1 (unknown, assumed infinite)
     *
     * @param config a ShardQueryConfiguration
     * @param node   a JexlNode
     * @return The list of index info ranges
     */
    public static List<Tuple2<String, IndexInfo>> createFullFieldIndexScanList(ShardQueryConfiguration config, JexlNode node) {
        List<Tuple2<String, IndexInfo>> list = new ArrayList<>();

        Calendar start = getCalendarStartOfDay(config.getBeginDate());
        Calendar end = getCalendarStartOfDay(config.getEndDate());

        while (start.compareTo(end) <= 0) {
            String day = DateHelper.format(start.getTime());
            IndexInfo info = new IndexInfo(-1);
            info.setNode(node);
            list.add(Tuples.tuple(day, info));
            start.add(Calendar.DAY_OF_YEAR, 1);
        }
        return list;
    }

    private static Calendar getCalendarStartOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    /**
     * This will create a list of index info (ranges) for the specified array of shards and days.
     *
     * @param shardsAndDays of shards and days
     * @return The list of index info ranges
     */
    public static List<Tuple2<String, IndexInfo>> createIndexScanList(String[] shardsAndDays) {
        List<Tuple2<String, IndexInfo>> list = new ArrayList<>();
        Arrays.sort(shardsAndDays);
        for (String shardOrDay : shardsAndDays) {
            IndexInfo info = new IndexInfo(-1);
            // create a new assignment node with just this shardOrDay
            JexlNode newNode = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(Constants.SHARD_DAY_HINT, shardOrDay));
            info.setNode(newNode);
            list.add(Tuples.tuple(shardOrDay, info));
        }
        return list;
    }

    /**
     * Setter for limit scanners
     *
     * @param limitScanners flag for the limit scanners
     * @return the range stream
     */
    public RangeStream setLimitScanners(final boolean limitScanners) {
        this.limitScanners = limitScanners;
        return this;
    }

    public boolean limitedScanners() {
        return limitScanners;
    }

    public void setMaxScannerBatchSize(int maxScannerBatchSize) {
        this.maxScannerBatchSize = maxScannerBatchSize;
    }

    public int getMaxScannerBatchSize() {
        return maxScannerBatchSize;
    }

    public UidIntersector getUidIntersector() {
        return uidIntersector;
    }

    public RangeStream setUidIntersector(UidIntersector uidIntersector) {
        this.uidIntersector = uidIntersector;
        return this;
    }

    public Class<? extends SortedKeyValueIterator<Key, Value>> getCreateUidsIteratorClass() {
        return createUidsIteratorClass;
    }

    public RangeStream setCreateUidsIteratorClass(Class<? extends SortedKeyValueIterator<Key, Value>> createUidsIteratorClass) {
        this.createUidsIteratorClass = createUidsIteratorClass;
        return this;
    }

    protected Set<String> getAllFieldsFromHelper() throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        if (this.helperAllFieldsCache.isEmpty()) {
            this.helperAllFieldsCache = this.metadataHelper.getAllFields(this.config.getDatatypeFilter());
        }
        return this.helperAllFieldsCache;
    }

    public static boolean isEventSpecific(Range range) {
        Text holder = new Text();
        Key startKey = range.getStartKey();
        startKey.getColumnFamily(holder);
        if (holder.getLength() > 0) {
            if (holder.find("\0") > 0) {
                return true;
            }
        }
        return false;
    }

    protected boolean containsIndexOnlyFields(JexlNode node) throws TableNotFoundException {
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);

        Set<String> indexOnlyFields = metadataHelper.getIndexOnlyFields(config.getDatatypeFilter());

        // Hack to get around the extra ASTIdentifier left in the AST by the threshold marker node
        Iterator<ASTIdentifier> iter = identifiers.iterator();
        while (iter.hasNext()) {
            ASTIdentifier id = iter.next();
            if (ExceededValueThresholdMarkerJexlNode.label().equals(id.image) || ExceededTermThresholdMarkerJexlNode.label().equals(id.image)
                    || ExceededOrThresholdMarkerJexlNode.label().equals(id.image)) {
                iter.remove();
            }
        }

        for (ASTIdentifier identifier : identifiers) {
            String fieldName = JexlASTHelper.deconstructIdentifier(identifier);

            if (indexOnlyFields.contains(fieldName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        streamExecutor.shutdownNow();
        executor.shutdownNow();
    }
}
