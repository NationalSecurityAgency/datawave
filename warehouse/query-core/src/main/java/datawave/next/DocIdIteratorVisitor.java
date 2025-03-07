package datawave.next;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
import org.apache.commons.lang3.LongRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.next.stats.DocumentIteratorStats;
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
 * <li>negated regex (NR)</li>
 * <li>negated equality (NE)</li>
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

    private final DocumentIteratorStats stats = new DocumentIteratorStats();

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
    @SuppressWarnings("unchecked")
    public static Set<Key> getDocIds(ASTJexlScript script, Range range, SortedKeyValueIterator<Key,Value> source, Set<String> datatypeFilter,
                    LongRange timeFilter, Set<String> indexedFields) {
        DocIdIteratorVisitor visitor = new DocIdIteratorVisitor(source, range, datatypeFilter, timeFilter, indexedFields);
        Object o = script.jjtAccept(visitor, null);
        if (o instanceof Set) {
            return (Set<Key>) o;
        }
        return Collections.emptySet();
    }

    public Set<Key> getDocIds(ASTJexlScript script) {
        Object o = script.jjtAccept(this, null);
        if (o instanceof Set) {
            return (Set<Key>) o;
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
    @SuppressWarnings("unchecked")
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

        if (!positive.isEmpty() && !negative.isEmpty()) {
            log.warn("union of negated and positive terms will not be executed");
            return null;
        }

        if (positive.isEmpty() && !negative.isEmpty()) {
            log.warn("union of negated terms will not be executed");
            return null;
        }

        Set<Key> ids = null;
        for (JexlNode child : positive) {
            Object o = child.jjtAccept(this, ids);
            if (o instanceof Set) {
                if (ids == null) {
                    ids = (Set<Key>) o;
                }
                ids.addAll((Set<Key>) o);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Node did not return a set: {}", JexlStringBuildingVisitor.buildQuery(child));
                }
            }
        }

        if (ids == null) {
            // no term was executable
            if (log.isDebugEnabled()) {
                log.debug("union: [{}] found 0 hits", JexlStringBuildingVisitor.buildQuery(node));
            }
            return data;
        }

        if (log.isDebugEnabled()) {
            log.debug("union: [{}] found {} hits", JexlStringBuildingVisitor.buildQuery(node), ids.size());
        }
        return ids;
    }

    /*
     * There are many potential types of joins happening here. Enumerate and work through cases.
     */

    @Override
    @SuppressWarnings("unchecked")
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isAnyType()) {
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
        Set<Key> ids = null;
        for (JexlNode child : positive) {
            Object o = child.jjtAccept(this, ids);
            if (!(o instanceof Set)) {
                if (log.isDebugEnabled()) {
                    log.debug("Node did not return a set: {}", JexlStringBuildingVisitor.buildQuery(child));
                }
                continue;
            }

            Set<Key> childIds = (Set<Key>) o;
            if (childIds.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("short circuit intersection, child returned zero hits");
                }
                return new HashSet<>();
            }

            if (ids == null) {
                ids = new HashSet<>(childIds);
            } else {
                ids.retainAll(childIds);
                if (ids.isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("short circuit intersection, no ids exist after merge");
                    }
                    return ids;
                }
            }
        }

        // TODO: handle the case of all negations (A && (B || (!C && !D)))

        // now process negations
        for (JexlNode child : negative) {
            Object o = child.jjtAccept(this, ids);
            if (!(o instanceof Set)) {
                if (log.isDebugEnabled()) {
                    log.debug("Node did not return a set: {}", JexlStringBuildingVisitor.buildQuery(child));
                }
                continue;
            }

            Set<Key> childIds = (Set<Key>) o;
            if (childIds.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("negated term in intersection, child returned zero hits");
                }
                continue;
            }

            // uncomment for exceptions
            // Preconditions.checkNotNull(ids);
            if (ids != null && !ids.isEmpty()) {
                ids.removeAll(childIds);
            }

            if (ids != null && ids.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("no ids exist for intersection after processing merge, short circuit return");
                }
                return ids;
            }
        }

        if (ids == null) {
            // no terms were executable
            if (log.isDebugEnabled()) {
                log.debug("intersection: [{}] found 0 hits", JexlStringBuildingVisitor.buildQuery(node));
            }
            return data;
        }
        if (log.isDebugEnabled()) {
            log.debug("intersection: [{}] found {} hits", JexlStringBuildingVisitor.buildQuery(node), ids.size());
        }
        return ids;
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
            case EXCEEDED_TERM:
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
        return configureAndDriveIterator(iterator);
    }

    private Object handleListMarker(ASTAndNode node, Object data, QueryPropertyMarker.Instance instance) {
        ListDocIdIterator iterator = new ListDocIdIterator(source, row, node);
        return configureAndDriveIterator(iterator);
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        if (field == null || !indexedFields.contains(field)) {
            return null; // do not execute iterators for non-indexed fields
        }

        Object value = JexlASTHelper.getLiteralValue(node);
        if (value == null) {
            return null; // do not execute iterators for terms like 'FIELD == null'
        }

        DocIdIterator iterator = new DocIdIterator(source, row, node);
        return configureAndDriveIterator(iterator);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        RegexDocIdIterator iterator = new RegexDocIdIterator(source, row, node);
        return configureAndDriveIterator(iterator);
    }

    protected Set<Key> configureAndDriveIterator(BaseDocIdIterator iterator) {
        if (datatypeFilter != null) {
            iterator.withDatatypes(datatypeFilter);
        }
        iterator.withTimeFilter(timeFilter);
        if (isDocRange) {
            iterator.withSuffix(getSuffix());
        }

        // TODO: if another anchor term exists in the query, allow this scan to timeout
        Set<Key> ids = new HashSet<>();
        while (iterator.hasNext()) {
            ids.add(iterator.next());
        }

        stats.merge(iterator.getStats());

        if (log.isDebugEnabled()) {
            log.debug("term: [{}] found {} hits", iterator.getNode(), ids.size());
        }
        return ids;
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

    public DocumentIteratorStats getStats() {
        return stats;
    }
}
