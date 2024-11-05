package datawave.query.index.lookup;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_TERM;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.INDEX_HOLE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.language.parser.jexl.JexlNodeSet;
import datawave.query.util.count.CountMap;

/**
 * This class represents information about hits in the index.
 * <p>
 * Hits may be represented by individual document ids or by a simple count.
 * <p>
 * The IndexInfo object supports union and intersection operations with other IndexInfo objects.
 */
public class IndexInfo implements Writable, UidIntersector {

    private static final Logger log = Logger.getLogger(IndexInfo.class);

    protected JexlNode myNode = null;
    // a count of -1 indicates a shard range that datawave created
    // a positive count indicates the number of uids added for this term
    protected long count;
    // a set of document uids. In some cases this list is pruned when a threshold is exceeded
    // In the pruned case, the count will exceed the size of the uid set
    protected ImmutableSortedSet<IndexMatch> uids;

    protected CountMap fieldCounts = new CountMap();
    protected CountMap termCounts = new CountMap();

    public IndexInfo() {
        this.count = 0;
        this.uids = ImmutableSortedSet.of();
    }

    public IndexInfo(long count) {
        this.count = count;
        this.uids = ImmutableSortedSet.of();
    }

    public IndexInfo(Iterable<?> ids) {
        Set<IndexMatch> matches = Sets.newTreeSet();
        for (Object id : ids) {
            if (id instanceof IndexMatch) {
                matches.add((IndexMatch) id);
            } else
                matches.add(new IndexMatch(id.toString()));
        }
        this.uids = ImmutableSortedSet.copyOf(matches);
        this.count = this.uids.size();
    }

    public boolean onlyEvents() {
        return count > 0 && count == uids.size();
    }

    public long count() {
        return count;
    }

    public ImmutableSortedSet<IndexMatch> uids() {
        return uids;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new VLongWritable(count).write(out);
        new VIntWritable(uids.size()).write(out);
        for (IndexMatch uid : uids) {
            uid.write(out);
        }
        MapWritable fieldMapWritable = new MapWritable();
        for (Map.Entry<String,Long> entry : fieldCounts.entrySet()) {
            fieldMapWritable.put(new Text(entry.getKey()), new VLongWritable(entry.getValue()));
        }
        fieldMapWritable.write(out);

        MapWritable termMapWritable = new MapWritable();
        for (Map.Entry<String,Long> entry : termCounts.entrySet()) {
            termMapWritable.put(new Text(entry.getKey()), new VLongWritable(entry.getValue()));
        }
        termMapWritable.write(out);
    }

    public void applyNode(JexlNode node) {
        JexlNode copy = RebuildingVisitor.copy(node);
        copy.jjtSetParent(null);
        myNode = copy;
        for (IndexMatch match : uids) {
            match.add(node);
        }
    }

    public JexlNode getNode() {
        return myNode;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        VLongWritable count = new VLongWritable();
        count.readFields(in);
        this.count = count.get();

        VIntWritable nUidsReader = new VIntWritable();
        nUidsReader.readFields(in);
        final int nUids = nUidsReader.get();

        ImmutableSortedSet.Builder<IndexMatch> setBuilder = ImmutableSortedSet.naturalOrder();

        for (int i = 0; i < nUids; ++i) {
            IndexMatch index = new IndexMatch();
            index.readFields(in);
            setBuilder.add(index);
        }
        this.uids = setBuilder.build();

        MapWritable fieldMapWritable = new MapWritable();
        fieldMapWritable.readFields(in);
        this.fieldCounts = new CountMap();
        for (Writable key : fieldMapWritable.keySet()) {
            fieldCounts.put(key.toString(), Long.valueOf(fieldMapWritable.get(key).toString()));
        }

        MapWritable termMapWritable = new MapWritable();
        termMapWritable.readFields(in);
        this.termCounts = new CountMap();
        for (Writable key : termMapWritable.keySet()) {
            termCounts.put(key.toString(), Long.valueOf(termMapWritable.get(key).toString()));
        }
    }

    public IndexInfo union(IndexInfo o) {
        return union(o, new ArrayList<>());
    }

    /**
     * Let's be clear about what we are doing. In this case we are dealing with a union of many nodes, or in some cases a single OrNode. If the latter, we
     * simply use that node as our node. Otherwise, we will need to create an or node manually.
     *
     * @param first
     *            an IndexInfo
     * @param o
     *            another IndexInfo
     * @param delayedNodes
     *            a list of delayed terms
     * @return the union of two IndexInfo objects and the list of delayed nodes
     */
    public IndexInfo union(IndexInfo first, IndexInfo o, List<JexlNode> delayedNodes) {
        IndexInfo merged = new IndexInfo();
        JexlNodeSet nodeSet = new JexlNodeSet();

        if (null != first.myNode && first.myNode != o.myNode) {

            JexlNode sourceNode = getSourceNode(first.myNode);
            JexlNode topLevelOr = getOrNode(sourceNode);

            if (null == topLevelOr) {
                nodeSet.add(sourceNode);
            } else {
                for (int i = 0; i < topLevelOr.jjtGetNumChildren(); i++) {
                    nodeSet.add(topLevelOr.jjtGetChild(i));
                }
            }
        }

        if (null != o.myNode) {
            JexlNode sourceNode = getSourceNode(o.myNode);
            JexlNode topLevelOr = getOrNode(sourceNode);

            if (null == topLevelOr) {
                nodeSet.add(o.myNode);
            } else {
                for (int i = 0; i < topLevelOr.jjtGetNumChildren(); i++) {
                    nodeSet.add(topLevelOr.jjtGetChild(i));
                }
            }
        }

        // Add delayed nodes.
        nodeSet.addAll(delayedNodes);

        merged.count = -1;
        merged.uids = ImmutableSortedSet.of();
        if (nodeSet.isEmpty()) {
            merged.myNode = null;
        } else {
            merged.myNode = JexlNodeFactory.createOrNode(nodeSet.getNodes());
        }

        merged.setFieldCounts(first.getFieldCounts());
        merged.mergeFieldCounts(o.getFieldCounts());

        merged.setTermCounts(first.getTermCounts());
        merged.mergeTermCounts(o.getTermCounts());

        return merged;
    }

    private JexlNode getOrNode(JexlNode node) {
        if (node instanceof ASTOrNode) {
            return node;
        } else if (node instanceof ASTReferenceExpression) {
            return getOrNode(node.jjtGetChild(0));
        }
        return null;
    }

    public static JexlNode getSourceNode(JexlNode delayedNode) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(delayedNode);
        if (instance.isAnyTypeOf(DELAYED, EXCEEDED_OR, EXCEEDED_VALUE, EXCEEDED_TERM, INDEX_HOLE)) {
            return instance.getSource();
        } else {
            return delayedNode;
        }
    }

    /**
     * Return the union of an IndexInfo object and a list of delayed nodes.
     *
     * @param o
     *            an IndexInfo object
     * @param delayedNodes
     *            a list of delayed nodes
     * @return the resulting union
     */
    public IndexInfo union(IndexInfo o, List<JexlNode> delayedNodes) {

        if (isInfinite()) {
            return union(this, o, delayedNodes);
        } else if (o.isInfinite()) {
            return union(o, this, delayedNodes);
        }

        IndexInfo merged = new IndexInfo();
        JexlNodeSet nodeSet = new JexlNodeSet();

        if (null != myNode && myNode != o.myNode && !isInfinite()) {
            JexlNode sourceNode = getSourceNode(myNode);

            JexlNode topLevelOr = getOrNode(sourceNode);
            if (null == topLevelOr) {
                nodeSet.add(myNode);
            } else {
                for (int i = 0; i < topLevelOr.jjtGetNumChildren(); i++) {
                    nodeSet.add(topLevelOr.jjtGetChild(i));
                }
            }
        }

        if (null != o.myNode && !o.isInfinite()) {

            JexlNode sourceNode = getSourceNode(o.myNode);
            JexlNode topLevelOr = getOrNode(sourceNode);
            if (null == topLevelOr) {
                nodeSet.add(o.myNode);
            } else {
                for (int i = 0; i < topLevelOr.jjtGetNumChildren(); i++) {
                    nodeSet.add(topLevelOr.jjtGetChild(i));
                }
            }
        }

        // Add delayed nodes.
        nodeSet.addAll(delayedNodes);

        if (!onlyEvents() || !o.onlyEvents()) {
            /*
             * We are dealing with high cardinality terms. Sum the counts and return a parent node.
             */
            merged.count = count + o.count;
            merged.uids = ImmutableSortedSet.of();
        } else {
            HashMultimap<String,JexlNode> ids = HashMultimap.create();

            /*
             * Concatenate all UIDs and merge the individual nodes
             */
            for (IndexMatch match : Iterables.concat(uids, o.uids)) {

                JexlNode newNode = match.getNode();
                if (null != newNode)
                    ids.put(match.uid, newNode);
            }

            Set<IndexMatch> matches = Sets.newHashSet();

            for (String uid : ids.keySet()) {
                Set<JexlNode> nodes = Sets.newHashSet(ids.get(uid));
                if (!nodes.isEmpty()) {
                    nodes.addAll(delayedNodes);
                    matches.add(new IndexMatch(nodes, uid, IndexMatchType.OR));
                }

            }
            merged.uids = ImmutableSortedSet.copyOf(matches);
            merged.count = merged.uids.size();
        }

        if (this == o) {
            // handle idiosyncrasy of the peeking iterator where the first term is merged with itself
            merged.setFieldCounts(o.getFieldCounts());
            merged.setTermCounts(o.getTermCounts());
        } else {
            merged.setFieldCounts(getFieldCounts());
            merged.setTermCounts(getTermCounts());

            merged.mergeFieldCounts(o.getFieldCounts());
            merged.mergeTermCounts(o.getTermCounts());
        }

        /*
         * If there are multiple levels within a union we could have an ASTOrNode. We cannot prune OrNodes as we would with an intersection, so propagate the
         * OrNode.
         */
        if (log.isTraceEnabled()) {
            for (String node : nodeSet.getNodeKeys()) {
                log.trace("internalNodeList node  is " + node);
            }
        }
        if (nodeSet.isEmpty()) {
            merged.myNode = null;
        } else {
            merged.myNode = TreeFlatteningRebuildingVisitor.flatten(JexlNodeFactory.createOrNode(nodeSet.getNodes()));
        }
        return merged;
    }

    /**
     * Find the intersection of a list of delayed nodes.
     *
     * @param delayedNodes
     *            a list of delayed nodes.
     * @return true if any child nodes were added to this IndexInfo object.
     */
    public boolean intersect(List<JexlNode> delayedNodes) {

        if (!onlyEvents() || isInfinite()) {
            return false;
        }
        for (IndexMatch match : uids) {
            JexlNode newNode = match.getNode();
            if (null == newNode)
                continue;

            Set<JexlNode> nodeSet = Sets.newHashSet(delayedNodes);
            nodeSet.add(match.getNode());

            match.set(TreeFlatteningRebuildingVisitor.flatten(JexlNodeFactory.createAndNode(nodeSet)));
            // TODO this may need to be of type AND for nested logic to be correct
        }

        if (null != myNode || null != delayedNodes) {
            Set<JexlNode> internalNodeList = Sets.newHashSet();
            if (null != myNode)
                internalNodeList.add(myNode);
            if (null != delayedNodes)
                internalNodeList.addAll(delayedNodes);

            if (!internalNodeList.isEmpty()) {
                myNode = TreeFlatteningRebuildingVisitor.flatten(JexlNodeFactory.createAndNode(internalNodeList));
            }
        }

        return true;
    }

    public IndexInfo intersect(IndexInfo o) {
        return intersect(o, new ArrayList<>(), this);
    }

    /**
     * The intersection logic is relatively straightforward as we only have three types of nodes
     * <ol>
     * <li>{@code INFINITE -- assume a hit exists}</li>
     * <li>{@code LARGE -- uids > 20, were pruned and a count remains}</li>
     * <li>{@code SMALL -- uids < 20}</li>
     * </ol>
     * See {@link #isInfinite()} for the definition of an infinite range.
     * <p>
     * We retain uids if and only if uids exist on both sides of the intersection
     * <p>
     * A uid and shard range will always produce a shard range. Never assume the uid hit exists on the other side.
     * <p>
     * In the case of two infinite ranges, we persist the -1 count for posterity
     *
     * @param o
     *            another {@link IndexInfo}
     * @param delayedNodes
     *            a list of delayed terms
     * @param uidIntersector
     *            a class that helps intersect uids
     * @return the result of an intersection operation on this IndexInfo and the other IndexInfo
     */
    public IndexInfo intersect(IndexInfo o, List<JexlNode> delayedNodes, UidIntersector uidIntersector) {

        // infinite = (count == -1)
        // onlyEvents = (count == uids.size())

        IndexInfo merged = new IndexInfo();

        if (onlyEvents() && o.onlyEvents()) {

            // if both sides are uid-only perform an intersection on the uids, propagating delayed nodes
            merged.uids = ImmutableSortedSet.copyOf(uidIntersector.intersect(uids, o.uids, delayedNodes));
            merged.count = merged.uids.size();

        } else if (isInfinite() && o.isInfinite()) {

            // if both sides are infinite ranges generated by us (-1 count), persist the -1 count
            merged.count = -1;
            merged.uids = ImmutableSortedSet.of();

        } else {

            // else we are left countably infinite shard ranges. Take the minimum count.
            merged.count = Math.min(count, o.count);
            merged.uids = ImmutableSortedSet.of();
        }

        merged.setFieldCounts(this.getFieldCounts());
        merged.mergeFieldCounts(o.getFieldCounts());

        merged.setTermCounts(this.getTermCounts());
        merged.mergeTermCounts(o.getTermCounts());

        // now handle updating the top level node
        JexlNodeSet nodes = new JexlNodeSet();
        if (this.getNode() != null) {
            nodes.add(this.getNode());
        }
        if (o.getNode() != null) {
            nodes.add(o.getNode());
        }
        nodes.addAll(delayedNodes);
        merged.myNode = TreeFlatteningRebuildingVisitor.flatten(JexlNodeFactory.createAndNode(nodes.getNodes()));

        return merged;
    }

    @Override
    public Set<IndexMatch> intersect(Set<IndexMatch> uids1, Set<IndexMatch> uids2, List<JexlNode> delayedNodes) {
        HashMultimap<String,JexlNode> ids = HashMultimap.create();
        for (IndexMatch match : Iterables.concat(uids1, uids2)) {
            JexlNode newNode = match.getNode();
            if (null != newNode)
                ids.put(match.uid, newNode);
        }

        // Do the actual merge of ids here; only ids with more than one JexlNode will make it through this method.
        return buildNodeList(ids, IndexMatchType.AND, false, delayedNodes);
    }

    protected Set<IndexMatch> buildNodeList(HashMultimap<String,JexlNode> ids, IndexMatchType type, boolean allowsDelayed, List<JexlNode> delayedNodes) {
        Set<IndexMatch> matches = Sets.newHashSet();
        for (String uid : ids.keySet()) {

            Set<JexlNode> nodes = ids.get(uid);
            // make sure that we have nodes, otherwise we are pruned to nothing
            if (nodes.size() > 1 || (allowsDelayed && (nodes.size() + delayedNodes.size()) > 1)) {
                JexlNodeSet nodeSet = new JexlNodeSet();
                nodeSet.addAll(nodes);
                nodeSet.addAll(delayedNodes);

                IndexMatch currentMatch = new IndexMatch(Sets.newHashSet(nodeSet.getNodes()), uid, type);
                matches.add(currentMatch);
            }
        }
        return matches;
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o instanceof IndexInfo) {
            IndexInfo other = (IndexInfo) o;
            return count() == other.count() && uids().equals(other.uids());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(count(), uids());
    }

    public String toString() {
        return "{ \"count\": " + count() + " - " + uids.size() + " }";
    }

    /**
     * An infinite range is one that datawave created. This can happen several ways
     * <ol>
     * <li>{@link RangeStream#createFullFieldIndexScanList(ShardQueryConfiguration, JexlNode)}</li>
     * <li>{@link RangeStream#createIndexScanList(String[])}</li>
     * <li>{@link ShardLimitingIterator#next()}</li>
     * <li>{@link Union} of all delayed terms</li>
     * <li>RangeStreamScanner if certain thresholds are exceeded</li>
     * </ol>
     *
     * @return true if the count is equal to negative one
     */
    private boolean isInfinite() {
        return count == -1L;
    }

    public void setNode(JexlNode currNode) {
        myNode = currNode;

        // need to update the nodes for the underlying index matches in order
        // to persist delayed terms. In the case of a document specific range
        // the query plan is pulled from indexMatch.getNode()
        for (IndexMatch uid : uids) {
            uid.set(myNode);
        }
    }

    public void setFieldCounts(CountMap fieldCounts) {
        this.fieldCounts.putAll(fieldCounts);
    }

    public void setTermCounts(CountMap termCounts) {
        this.termCounts.putAll(termCounts);
    }

    public void mergeFieldCounts(CountMap otherCounts) {
        if (fieldCounts == null || fieldCounts.isEmpty()) {
            fieldCounts = otherCounts;
            return;
        }

        for (String field : otherCounts.keySet()) {
            if (fieldCounts.containsKey(field)) {
                Long existingCount = fieldCounts.get(field);
                Long otherCount = otherCounts.get(field);
                fieldCounts.put(field, existingCount + otherCount);
            } else {
                fieldCounts.put(field, otherCounts.get(field));
            }
        }
    }

    public void mergeTermCounts(CountMap otherCounts) {
        if (termCounts == null || termCounts.isEmpty()) {
            termCounts = otherCounts;
            return;
        }

        for (String field : otherCounts.keySet()) {
            if (termCounts.containsKey(field)) {
                Long existingCount = termCounts.get(field);
                Long otherCount = otherCounts.get(field);
                termCounts.put(field, existingCount + otherCount);
            } else {
                termCounts.put(field, otherCounts.get(field));
            }
        }
    }

    public CountMap getFieldCounts() {
        return fieldCounts;
    }

    public CountMap getTermCounts() {
        return termCounts;
    }
}
