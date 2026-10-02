package datawave.next;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.commons.jexl3.parser.ASTJexlScript;
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
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * A visitor that scans the field index and returns all document ids that match a given query.
 * <p>
 * Operators that are supported
 * <ul>
 * <li>equality</li>
 * <li>regex</li>
 * <li>range</li>
 * <li>list marker</li>
 * <li>negations that are part of an intersection</li>
 * </ul>
 * <p>
 * Operators that are NOT supported
 * <ul>
 * <li>negated regex</li>
 * <li>negated equality</li>
 * <li>functions</li>
 * <li>term markers</li>
 * </ul>
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
     *
     * @param script
     *            the query tree
     * @param range
     *            the range
     * @param source
     *            the source iterator
     * @param datatypeFilter
     *            the datatype filter
     * @param timeFilter
     *            the time filter
     * @return the set of document ids that satisfy the query
     */
    public static Set<Key> getDocIds(ASTJexlScript script, Range range, SortedKeyValueIterator<Key,Value> source, Set<String> datatypeFilter,
                    LongRange timeFilter, Set<String> indexedFields) {
        DocIdIteratorVisitor visitor = new DocIdIteratorVisitor(source, range, datatypeFilter, timeFilter, indexedFields);
        Object o = script.jjtAccept(visitor, null);
        if (o instanceof ScanResult) {
            return ((ScanResult) o).getResults();
        }
        return Collections.emptySet();
    }

    public Set<Key> getDocIds(ASTJexlScript script) {
        Object o = script.jjtAccept(this, null);
        if (o instanceof ScanResult) {
            return ((ScanResult) o).getResults();
        }
        return Collections.emptySet();
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

        ScanResult context = data instanceof ScanResult ? (ScanResult) data : null;
        if (!negative.isEmpty() && context == null) {
            log.trace("union containing negated terms cannot be executed without external context");
            return null;
        }

        ScanResult result = null;
        boolean hasUnsupportedPositive = false;
        for (JexlNode child : positive) {
            // union passes in external context
            Object o = child.jjtAccept(this, data);
            if (o instanceof ScanResult && o != data) {
                ScanResult scanResult = (ScanResult) o;
                if (result == null) {
                    result = scanResult;
                } else {
                    result.union(scanResult);
                }
            } else {
                hasUnsupportedPositive = true;
                if (log.isTraceEnabled()) {
                    log.trace("Node did not return a set: {}", JexlStringBuildingVisitor.buildQuery(child));
                }
            }
        }

        for (JexlNode child : negative) {
            // The enclosing intersection supplies the universe for a negated union branch.
            ScanResult complement = copyForComplement(context);
            Object o = child.jjtAccept(this, context);
            // A nested expression that cannot be executed returns its input context. A partially
            // executable composite returns candidates marked as unsafe to subtract.
            if (o instanceof ScanResult && o != context) {
                complement.subtractConfirmedMatches((ScanResult) o);
            } else {
                complement.setSubtractionSafe(false);
            }

            if (result == null) {
                result = complement;
            } else {
                result.union(complement);
            }
        }

        if (result != null && hasUnsupportedPositive) {
            // Executable union branches are confirmed matches, but they are not the complete union
            // when another branch could not be evaluated.
            result.setMatchComplete(false);
        }

        if (result != null) {
            // A composite union cannot safely use a leaf iterator's source type for partial intersection.
            result.setSource(null);
        }

        if (result == null) {
            // no term was executable
            if (log.isTraceEnabled()) {
                log.trace("union: [{}] found 0 hits", JexlStringBuildingVisitor.buildQuery(node));
            }
            return data;
        }

        if (log.isTraceEnabled()) {
            log.trace("union: [{}] found {} hits", JexlStringBuildingVisitor.buildQuery(node), result.getResults().size());
        }
        return result;
    }

    private ScanResult copyForComplement(ScanResult source) {
        ScanResult copy = new ScanResult(allowPartialIntersections);
        copy.addKeys(source.getResults());
        copy.setSubtractionSafe(source.isSubtractionSafe());
        copy.setMatchComplete(source.isMatchComplete());
        copy.setTimeout(source.isTimeout());
        return copy;
    }

    /*
     * There are many potential types of joins happening here. Enumerate and work through cases.
     */

    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isAnyType()) {
            // always pass context to a marker node. If the parent is a union, the only way context was passed in is because context exists from a grandparent
            // intersection as in the case of (A and (B or marker))
            return handleMarker(node, data, instance);
        }

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

        // positive terms first
        ScanResult result = null;
        boolean hasUnsupportedPositive = false;
        for (JexlNode child : positive) {
            // intersections drive their own context
            Object scanContext = result;
            if (result != null && !result.isMatchComplete()) {
                scanContext = data instanceof ScanResult && ((ScanResult) data).isMatchComplete() ? data : null;
            }
            Object o = child.jjtAccept(this, scanContext);
            if (!(o instanceof ScanResult) || o == scanContext) {
                hasUnsupportedPositive = true;
                if (log.isDebugEnabled()) {
                    log.debug("Node did not return a set: {}", JexlStringBuildingVisitor.buildQuery(child));
                }
                continue;
            }

            ScanResult scanResult = (ScanResult) o;
            if (scanResult.getResults().isEmpty() && scanResult.isMatchComplete()) {
                if (log.isDebugEnabled()) {
                    log.debug("short circuit intersection, child returned zero hits");
                }
                return scanResult;
            }

            if (result == null) {
                result = scanResult;
            } else {
                // scan results know how to handle partial intersections, as in the case of a timeout
                result.intersect(scanResult);

                if (result.getResults().isEmpty() && result.isMatchComplete()) {
                    if (log.isDebugEnabled()) {
                        log.debug("short circuit intersection, no ids exist after merge");
                    }
                    return result;
                }
            }
        }

        if (result != null && hasUnsupportedPositive) {
            // Executable children still provide useful candidates, but they do not prove that the
            // candidates satisfy this conjunction when another positive child was not evaluated.
            result.setSubtractionSafe(false);
        }

        if (result != null && result.getResults().isEmpty()) {
            // An incomplete empty candidate set cannot safely bound a negated child. Return it to an enclosing intersection, where a complete anchor can
            // replace it with a conservative candidate universe.
            return result;
        }

        // TODO: handle the case of all negations (A && (B || (!C && !D)))

        // now process negations
        for (JexlNode child : negative) {
            // intersections drive their own context
            Object o = child.jjtAccept(this, result);
            if (!(o instanceof ScanResult) || o == result) {
                if (result != null) {
                    result.setSubtractionSafe(false);
                }
                if (log.isDebugEnabled()) {
                    log.debug("Node did not return a set: {}", JexlStringBuildingVisitor.buildQuery(child));
                }
                continue;
            }

            ScanResult scanResult = (ScanResult) o;
            if (result != null && !result.getResults().isEmpty()) {
                result.subtractConfirmedMatches(scanResult);
            }

            if (scanResult.getResults().isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("negated term in intersection, child returned zero hits");
                }
                continue;
            }

            // uncomment for exceptions
            // Preconditions.checkNotNull(ids);
            if (result != null && result.getResults().isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("no ids exist for intersection after processing merge, short circuit return");
                }
                return result;
            }
        }

        if (result == null) {
            // no terms were executable
            if (log.isDebugEnabled()) {
                log.debug("intersection: [{}] found 0 hits", JexlStringBuildingVisitor.buildQuery(node));
            }
            return data;
        }
        if (log.isDebugEnabled()) {
            log.debug("intersection: [{}] found {} hits", JexlStringBuildingVisitor.buildQuery(node), result.getResults().size());
        }
        return result;
    }

    /**
     * This method exists because we may have a bounded range that is also marked as value exceeded
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
                return data;
            case EVALUATION_ONLY:
            case DELAYED:
            case DROPPED:
            case STRICT:
            case LENIENT:
                log.debug("not handling marker of type: {}", instance.getType().getLabel());
                return data;
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

    @Override
    public Object visit(ASTNotNode node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
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
