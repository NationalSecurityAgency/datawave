package datawave.next;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.next.stats.DocIterStats;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.RewriteNegationsVisitor;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * A visitor that scans the field index and returns all document ids that match a given query.
 * <p>
 * Operators that are supported
 * <ul>
 * <li>equality</li>
 * <li>regex</li>
 * <li>range, as a bounded range marker</li>
 * <li>list marker</li>
 * <li>negations, in any position an enclosing intersection can subtract them from</li>
 * </ul>
 * <p>
 * Operators that are NOT supported
 * <ul>
 * <li>functions</li>
 * <li>term markers</li>
 * </ul>
 * <p>
 * Every node returns a {@link ScanResult} bounding the documents its subtree can match, or null when the field index cannot bound it at all, and the parent
 * composes those bounds with {@link ScanResult#and(ScanResult, ScanResult)}, {@link ScanResult#or(ScanResult, ScanResult)} and
 * {@link ScanResult#negate(ScanResult)}. Nothing returns the incoming context: a negated parent treats whatever it is handed as documents to remove, so echoing
 * the context back would have that parent subtract its own result from itself and find nothing.
 * <p>
 * A bound is always widened rather than narrowed when precision is lost, so the visitor can return a document the query does not match, which is then discarded
 * by evaluating the document itself, but never drops one the query does match.
 * <p>
 * A negated equality or negated regex is neither supported nor expected. The {@link RewriteNegationsVisitor} turns {@code A != B} into {@code !(A == B)} and
 * {@code A !~ B} into {@code !(A =~ B)} while planning the query, so reaching one here means the tree was never planned and the scan fails rather than silently
 * returning the wrong document ids.
 */
public class DocIdIteratorVisitor extends BaseVisitor {

    private static final Logger log = LoggerFactory.getLogger(DocIdIteratorVisitor.class);

    private final Range range;
    private final SortedKeyValueIterator<Key,Value> source;
    private final Set<String> datatypeFilter;
    private final LongRange timeFilter;
    private final Set<String> indexedFields;

    private final String row;
    private boolean isDocRange = false;

    private long maxScanTimeMillis = 15_000L;
    private long resultInterval = 500;
    private boolean allowPartialIntersections = false;

    private final DocIterStats stats = new DocIterStats();

    private final Clock clock = Clock.systemUTC();

    /**
     * Scans the field index and returns the bound on the documents this query can match.
     * <p>
     * A caller must handle an unbounded result rather than treating it as an empty one. There is no key set that means "every document", so a query the field
     * index cannot bound needs a full table scan, which this stack does not perform. See {@link #getDocIds(ASTJexlScript)} for the callers that refuse instead.
     *
     * @param script
     *            the query tree
     * @return the bound, or null when the field index cannot bound the query at all
     */
    public ScanResult getScanResult(ASTJexlScript script) {
        Object o = script.jjtAccept(this, null);
        return o instanceof ScanResult ? (ScanResult) o : null;
    }

    /**
     * Scans the field index for the candidate document ids, which may be a superset of the documents the query matches. Evaluating each document discards the
     * extras, so a caller that does not evaluate them must check {@link ScanResult#isExact()} rather than answering from this set.
     *
     * @param script
     *            the query tree
     * @return the candidate document ids
     * @throws DatawaveFatalQueryException
     *             when the field index cannot bound the query, which would require a full table scan
     */
    public Set<Key> getDocIds(ASTJexlScript script) {
        ScanResult result = getScanResult(script);
        if (result == null || !result.isBounded()) {
            throw unboundedQuery(script);
        }
        return result.getResults();
    }

    /**
     * Builds the failure for a query the field index cannot bound.
     * <p>
     * Reporting no documents would be a silently wrong answer, so this fails instead. It should be unreachable: a query needing a full table scan is rejected
     * by the planner well before the field index sees it, provided full table scans are disabled.
     *
     * @param script
     *            the query tree
     * @return a fatal query exception
     */
    private DatawaveFatalQueryException unboundedQuery(ASTJexlScript script) {
        String msg = "Field index cannot bound the query, a full table scan would be required: [" + JexlStringBuildingVisitor.buildQuery(script) + "]";
        log.error(msg);
        return new DatawaveFatalQueryException(new QueryException(DatawaveErrorCode.FULL_TABLE_SCAN_REQUIRED_BUT_DISABLED, msg));
    }

    protected DocIdIteratorVisitor(SortedKeyValueIterator<Key,Value> source, Range range, Set<String> datatypeFilter, LongRange timeFilter,
                    Set<String> indexedFields) {
        this.source = source;
        this.range = range;
        this.datatypeFilter = datatypeFilter;
        this.timeFilter = timeFilter;
        this.indexedFields = indexedFields;

        this.row = range.getStartKey().getRow().toString();
        this.isDocRange = isDocRange(this.range);
    }

    private boolean isDocRange(Range range) {
        return range.isStartKeyInclusive() && range.getStartKey().getColumnFamily().getLength() > 0;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        ScanResult result = null;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = JexlASTHelper.dereference(node.jjtGetChild(i));

            // a union has no candidate set of its own to offer, so every child sees the external context
            ScanResult childResult = scan(child, context(data));
            if (childResult == null) {
                // an undecided disjunct could be true for any document, so the union bounds nothing
                if (log.isTraceEnabled()) {
                    log.trace("union: [{}] has an undecided term, no bound", JexlStringBuildingVisitor.buildQuery(node));
                }
                return null;
            }

            result = (i == 0) ? childResult : ScanResult.or(result, childResult);
            if (result == null) {
                return null;
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("union: [{}] bound {} keys", JexlStringBuildingVisitor.buildQuery(node), result.getResults().size());
        }
        return result;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isAnyType()) {
            // always pass context to a marker node. If the parent is a union, the only way context was passed in is because context exists from a grandparent
            // intersection as in the case of (A and (B or marker))
            return handleMarker(node, data, instance);
        }

        ScanResult result = null;
        boolean first = true;
        for (JexlNode child : positiveTermsFirst(node)) {
            // an intersection drives its own context, narrowing each scan by what the terms before it already bound
            ScanResult childResult = scan(child, context(result));
            result = first ? childResult : ScanResult.and(result, childResult);
            first = false;

            if (result != null && result.isBounded() && result.getResults().isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("short circuit intersection, no candidates remain");
                }
                return result;
            }
        }

        if (log.isDebugEnabled()) {
            if (result == null) {
                log.debug("intersection: [{}] has no decided term, no bound", JexlStringBuildingVisitor.buildQuery(node));
            } else {
                log.debug("intersection: [{}] bound {} keys", JexlStringBuildingVisitor.buildQuery(node), result.getResults().size());
            }
        }
        return result;
    }

    /**
     * Orders an intersection's children so that negations come last.
     * <p>
     * Purely a performance concern, since {@link ScanResult#and(ScanResult, ScanResult)} is associative and commutative. A negation cannot narrow the running
     * candidate set on its own, so running the positive terms first gives every negated scan a key range to stay inside.
     *
     * @param node
     *            an intersection
     * @return the dereferenced children, negations last
     */
    private List<JexlNode> positiveTermsFirst(ASTAndNode node) {
        List<JexlNode> positive = new ArrayList<>();
        List<JexlNode> negative = new ArrayList<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode deref = JexlASTHelper.dereference(node.jjtGetChild(i));
            if (deref instanceof ASTNotNode) {
                negative.add(deref);
            } else {
                positive.add(deref);
            }
        }
        positive.addAll(negative);
        return positive;
    }

    /**
     * Visits a node and returns its bound, treating anything that is not a {@link ScanResult} as undecided.
     * <p>
     * A node handing back the very context it was given has decided nothing, which is what {@link BaseVisitor} does for any node type this visitor does not
     * override. Every term that does compose a bound builds a new instance, so rejecting an echo costs nothing.
     *
     * @param node
     *            the node to visit
     * @param context
     *            the candidate set to scan within, or null
     * @return the node's bound, or null when this visitor cannot decide it
     */
    private ScanResult scan(JexlNode node, ScanResult context) {
        Object o = node.jjtAccept(this, context);
        if (o instanceof ScanResult && o != context) {
            return (ScanResult) o;
        }
        if (log.isTraceEnabled()) {
            log.trace("undecided term: {}", JexlStringBuildingVisitor.buildQuery(node));
        }
        return null;
    }

    /**
     * The candidate set a child may restrict its scan to.
     * <p>
     * Only a whole positive result qualifies. A negative result's keys are documents that cannot match, so its key range says nothing about where the
     * candidates are, and a partial scan's range stops wherever the scan was cut short.
     *
     * @param data
     *            the running result or incoming context, possibly null or not a ScanResult
     * @return the context to pass down, or null
     */
    private ScanResult context(Object data) {
        if (data instanceof ScanResult) {
            ScanResult result = (ScanResult) data;
            if (result.isBounded() && !result.isTimeout()) {
                return result;
            }
        }
        return null;
    }

    /**
     * This method exists because we may have a bounded range that is also marked as value exceeded
     * <p>
     * A marker that makes its source non-executable against the field index returns null, the same as any other term this visitor cannot resolve. It must not
     * return the incoming context: a negated term is handed the enclosing intersection's own running ScanResult, and echoing that back makes the intersection
     * subtract its result from itself and find nothing.
     *
     * @param node
     *            the original ASTAndNode
     * @param data
     *            the data
     * @param instance
     *            the QueryPropertyMarker Instance
     * @return an object
     */
    private Object handleMarker(ASTAndNode node, Object data, QueryPropertyMarker.Instance instance) {
        switch (instance.getType()) {
            case BOUNDED_RANGE:
                return handledBoundedRange(node, data, instance);
            case EXCEEDED_OR:
                return handleListMarker(node, data, instance);
            case EXCEEDED_VALUE:
                return handleExceededValue(node, data, instance);
            case INDEX_HOLE:
                log.info("found an index hole");
                return null;
            case STRICT:
            case LENIENT:
                // these only say how a missing field is evaluated, the source term is still scannable
                return scan(JexlASTHelper.dereference(instance.getSource()), context(data));
            case EVALUATION_ONLY:
            case DELAYED:
            case DROPPED:
                log.debug("not handling marker of type: {}", instance.getType().getLabel());
                return null;
            default:
                throw new RuntimeException("Unknown marker of type: " + instance.getType().getLabel());
        }
    }

    private Object handleExceededValue(ASTAndNode node, Object data, QueryPropertyMarker.Instance instance) {
        QueryPropertyMarker.Instance sourceInstance = QueryPropertyMarker.findInstance(instance.getSource());
        if (sourceInstance.isAnyType()) {
            return handleMarker((ASTAndNode) instance.getSource(), data, sourceInstance);
        }

        // delegate to visit(ASTERNode)
        Preconditions.checkNotNull(instance.getSource());
        return visit((ASTERNode) instance.getSource(), data);
    }

    private Object handledBoundedRange(ASTAndNode node, Object data, QueryPropertyMarker.Instance instance) {
        RangeDocIdIterator iterator = new RangeDocIdIterator(source, row, node);

        if (!isFieldIndexed(iterator.getField())) {
            return null; // do not execute iterators for non-indexed fields
        }

        return configureAndDriveIterator(iterator, data);
    }

    private Object handleListMarker(ASTAndNode node, Object data, QueryPropertyMarker.Instance instance) {
        ListDocIdIterator iterator = new ListDocIdIterator(source, row, node);

        if (!isFieldIndexed(iterator.getField())) {
            return null; // do not execute iterators for non-indexed fields
        }

        return configureAndDriveIterator(iterator, data);
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (!isFieldIndexed(node)) {
            return null; // do not execute iterators for non-indexed fields
        }

        Object value = JexlASTHelper.getLiteralValue(node);
        if (value == null) {
            return null; // do not execute iterators for terms like 'FIELD == null'
        }

        DocIdIterator iterator = new DocIdIterator(source, row, node);
        return configureAndDriveIterator(iterator, data);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        if (!isFieldIndexed(node)) {
            return null; // do not execute iterators for non-indexed fields
        }

        RegexDocIdIterator iterator = new RegexDocIdIterator(source, row, node);
        return configureAndDriveIterator(iterator, data);
    }

    /**
     * Determines if the field is indexed. The field should be found in the identifier.
     *
     * @param node
     *            the JexlNode
     * @return true if the field is indexed
     */
    private boolean isFieldIndexed(JexlNode node) {
        String field = JexlASTHelper.getIdentifier(node);
        return isFieldIndexed(field);
    }

    /**
     * Determines if the field is indexed. The field should be found in the identifier.
     *
     * @return true if the field is indexed
     */
    private boolean isFieldIndexed(String field) {
        return field != null && indexedFields.contains(field);
    }

    protected ScanResult configureAndDriveIterator(BaseDocIdIterator iterator, Object data) {
        if (datatypeFilter != null) {
            iterator.withDatatypes(datatypeFilter);
        }
        iterator.withTimeFilter(timeFilter);
        if (isDocRange) {
            iterator.withSuffix(getSuffix());
        }

        // check to see if this scan exists within the bounds of another scan
        if (data instanceof ScanResult) {
            ScanResult scanResult = (ScanResult) data;
            iterator.withMinMax(scanResult.getMin(), scanResult.getMax());
        }

        // if scan results exist then this scan is allowed to timeout
        boolean checkForTimeout = data instanceof ScanResult;
        long scanStart = clock.millis();
        long elapsedScanTime;

        int count = 0;
        ScanResult result = new ScanResult(allowPartialIntersections);
        result.updateSource(iterator);

        while (iterator.hasNext()) {
            count++;
            result.addKey(iterator.next());
            if (checkForTimeout && count % resultInterval == 0) {
                elapsedScanTime = clock.millis() - scanStart;
                if (elapsedScanTime >= maxScanTimeMillis) {
                    result.setTimeout(true);
                    log.warn("term: [{}] founds {} hits before hitting timeout threshold: {}", iterator.getNode(), result.getResults().size(),
                                    maxScanTimeMillis);
                    break;
                }
            }
        }

        elapsedScanTime = clock.millis() - scanStart;
        stats.merge(iterator.getStats());

        if (log.isDebugEnabled()) {
            log.debug("term: [{}] found {} hits in {} ms", iterator.getNode(), result.getResults().size(), elapsedScanTime);
        }
        return result;
    }

    private String getSuffix() {
        return range.getStartKey().getColumnFamily().toString();
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    /**
     * Complements the source term's bound. A negation cannot be scanned, only subtracted from a candidate set an enclosing intersection already has, so on its
     * own it returns a negative {@link ScanResult} that bounds nothing until it reaches that intersection.
     *
     * @param node
     *            an ASTNotNode
     * @param data
     *            the data
     * @return the complement of the source term's bound, or null when the source cannot be complemented
     */
    @Override
    public Object visit(ASTNotNode node, Object data) {
        JexlNode source = JexlASTHelper.dereference(node.jjtGetChild(0));
        return ScanResult.negate(scan(source, context(data)));
    }

    /**
     * The query planner rewrites {@code A !~ B} into {@code !(A =~ B)} via the {@link RewriteNegationsVisitor}, so this node cannot reach the field index.
     *
     * @param node
     *            an ASTNRNode
     * @param data
     *            the data
     * @return never returns
     * @throws DatawaveFatalQueryException
     *             always
     */
    @Override
    public Object visit(ASTNRNode node, Object data) {
        throw unrewrittenNegation(node);
    }

    /**
     * The query planner rewrites {@code A != B} into {@code !(A == B)} via the {@link RewriteNegationsVisitor}, so this node cannot reach the field index.
     *
     * @param node
     *            an ASTNENode
     * @param data
     *            the data
     * @return never returns
     * @throws DatawaveFatalQueryException
     *             always
     */
    @Override
    public Object visit(ASTNENode node, Object data) {
        throw unrewrittenNegation(node);
    }

    /**
     * Builds the failure for a negated operator that should have been rewritten before the query reached the field index.
     *
     * @param node
     *            the offending node
     * @return a fatal query exception
     */
    private DatawaveFatalQueryException unrewrittenNegation(JexlNode node) {
        String msg = "Negated operator was not rewritten by the " + RewriteNegationsVisitor.class.getSimpleName() + ": ["
                        + JexlStringBuildingVisitor.buildQuery(node) + "]";
        log.error(msg);
        return new DatawaveFatalQueryException(new QueryException(DatawaveErrorCode.UNEXPECTED_SOURCE_NODE, msg));
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return unboundedRange(node);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return unboundedRange(node);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return unboundedRange(node);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return unboundedRange(node);
    }

    /**
     * Refuses a range the field index cannot bound. A scannable range arrives as a bounded range marker, so a range operator reaching this visitor on its own
     * is open ended.
     *
     * @param node
     *            an unwrapped range operator
     * @return null, always
     */
    private Object unboundedRange(JexlNode node) {
        if (log.isTraceEnabled()) {
            log.trace("open ended range: [{}] is not scannable on its own", JexlStringBuildingVisitor.buildQuery(node));
        }
        return null;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return null;
    }

    public DocIterStats getStats() {
        return stats;
    }

    public void setMaxScanTimeMillis(long maxScanTimeMillis) {
        this.maxScanTimeMillis = maxScanTimeMillis;
    }

    public void setAllowPartialIntersections(boolean allowPartialIntersections) {
        this.allowPartialIntersections = allowPartialIntersections;
    }
}
