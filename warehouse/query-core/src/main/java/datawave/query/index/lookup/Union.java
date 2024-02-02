package datawave.query.index.lookup;

import static datawave.query.index.lookup.ShardEquality.isDay;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.language.parser.jexl.JexlNodeSet;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;
import datawave.util.StringUtils;

/**
 * Creates a union of global index range streams.
 *
 * This implementation of an IndexStream supports seeking to a specific shard. Such calls originate in the {@link Intersection}.
 */
public class Union extends BaseIndexStream {
    protected final PriorityQueue<IndexStream> children;
    protected final JexlNodeSet childNodes;
    protected final StreamContext context;
    protected final String contextDebug;
    protected final List<String> childrenContextDebug = new ArrayList<>();
    protected JexlNode currNode = null;
    protected JexlNodeSet delayedNodes;
    protected Tuple2<String,IndexInfo> next;

    private static final Logger log = Logger.getLogger(Union.class);

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Union(Iterable<? extends IndexStream> streams) {
        this.children = new PriorityQueue(16, PeekOrdering.make(new TupleComparator<>()));
        this.childNodes = new JexlNodeSet();
        this.delayedNodes = new JexlNodeSet();

        boolean delayedFromUnindexed = false; // did any delayed node come from an unindexed stream?

        for (IndexStream stream : streams) {

            if (log.isDebugEnabled()) {
                childrenContextDebug.add(stream.getContextDebug());
            }

            if (log.isTraceEnabled()) {
                log.trace("Union child " + JexlStringBuildingVisitor.buildQuery(stream.currentNode()) + " - " + stream.context() + " hasNext: "
                                + stream.hasNext());
            }

            if (stream.hasNext()) {
                switch (stream.context()) {
                    case PRESENT:
                    case VARIABLE:
                    case EXCEEDED_VALUE_THRESHOLD:
                        // index streams with data are always added
                        this.children.add(stream);
                        this.childNodes.add(stream.currentNode());
                        continue;
                    default:
                        throw new IllegalStateException("Unexpected stream context " + stream.context());
                }
            }

            switch (stream.context()) {
                case NO_OP:
                case ABSENT:
                    // these index streams are dropped from the union
                    continue;
                case UNINDEXED:
                    // a non-indexed field present in a top level union results in a non-executable query
                    delayedFromUnindexed = true;
                case IGNORED:
                case DELAYED_FIELD:
                case UNKNOWN_FIELD:
                case EXCEEDED_TERM_THRESHOLD:
                    // these nodes need to be persisted via the set of delayedNodes
                    delayedNodes.add(JexlNodes.wrap(stream.currentNode()));
                    continue;
                case INITIALIZED:
                    throw new RuntimeException("Invalid state in RangeStream");
                default:
                    throw new IllegalStateException("[Union] unhandled stream context: " + stream.context());
            }
        }

        // build the union based on updated information from the child index streams
        JexlNodeSet nodes = new JexlNodeSet();
        if (!childNodes.isEmpty()) {
            nodes.addAll(childNodes);
        }
        if (!delayedNodes.isEmpty()) {
            nodes.addAll(delayedNodes);
        }

        if (nodes.size() == 1) {
            currNode = nodes.iterator().next();
        } else {
            currNode = JexlNodeFactory.createOrNode(nodes.getNodes());
        }

        if (log.isTraceEnabled()) {
            log.trace("union has " + childNodes.size() + " active children and " + delayedNodes.size() + " delayed children");
        }

        if (childNodes.isEmpty() && delayedNodes.isEmpty()) {
            // both delayed and non-delayed nodes empty indicates an absent state
            this.context = StreamContext.ABSENT;
            this.contextDebug = "children are all absent";
        } else if (!childNodes.isEmpty() && !delayedNodes.isEmpty()) {
            // mix of delayed and non-delayed nodes indicates a variable state
            this.context = StreamContext.VARIABLE;
            this.contextDebug = "children are a mix of delayed and non-delayed";
        } else if (!childNodes.isEmpty() && delayedNodes.isEmpty()) {
            // only child nodes, no delayed
            this.context = StreamContext.PRESENT;
            this.contextDebug = "children are all present";
        } else {
            // only delayed nodes, figure out which context to surface
            if (delayedFromUnindexed) {
                this.context = StreamContext.UNINDEXED;
                this.contextDebug = "children contains at least one unindexed field";
            } else {
                this.context = StreamContext.DELAYED_FIELD;
                this.contextDebug = "children are all delayed";
            }
        }

        // advance the queue if we have active index streams
        if (!this.children.isEmpty()) {
            next();
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Tuple2<String,IndexInfo> next() {
        return next(null);
    }

    /**
     * Return the current result and search for the next result, optionally within a context
     *
     * @param context
     *            determines if this union needs to return a result matching the context
     * @return the current result
     */
    public Tuple2<String,IndexInfo> next(String context) {
        Tuple2<String,IndexInfo> ret = next;
        next = advanceQueue(context);
        return ret;
    }

    @Override
    public Tuple2<String,IndexInfo> peek() {
        return next;
    }

    /**
     * Advance the priority queue of index streams, optionally within the provided context.
     * <p>
     * If a context is provided then this union is being advanced by an intersection
     *
     * @param context
     *            optional parameter
     * @return the next valid shard
     */
    private Tuple2<String,IndexInfo> advanceQueue(String context) {
        if (children.isEmpty()) {
            return null;
        }
        Tuple2<String,IndexInfo> head = children.peek().peek();
        // shard can be either a specific shard or a day denoting all shards
        String dayOrShard = head.first();
        IndexInfo pointers = head.second();

        // check for edge case like 'A && (B || delayed_C)'
        // in this case term A hits on a shard that sorts before the next shard for term B
        // even though term B doesn't hit, this union should still return the delayed term
        if (context != null && ShardEquality.lessThan(context, dayOrShard) && !delayedNodes.isEmpty()) {
            return buildHitFromDelayedTerms(context);
        }

        // use startsWith to match shards with a day
        if (log.isTraceEnabled())
            log.trace("advancing " + pointers.getNode() + " " + children.peek().context());

        /**
         * Since children is a PriorityQueue sorted by shard and its possible for a day_shard to call next() and then return a day. That iterator will sort to
         * the front. This may cause a break in the end loop condition before all iterators have had a chance to be evaluated. Until all iterators have been
         * evaluated or the end condition is reached don't evaluate an iterator a second time.
         */
        List<IndexStream> processedChildren = new ArrayList<>();
        while (!children.isEmpty()) {
            String streamDayOrShard = children.peek().peek().first();
            if (log.isTraceEnabled())
                log.trace("we have " + streamDayOrShard + " " + dayOrShard);

            if (!streamDayOrShard.equals(dayOrShard)) {
                // additional test if dayOrShard is a day
                if (!(isDay(dayOrShard) && streamDayOrShard.startsWith(dayOrShard))) {
                    // if there were children we previously processed, add them back in
                    if (processedChildren.size() > 0) {
                        children.addAll(processedChildren);
                        processedChildren.clear();
                        continue;
                    } else {
                        break;
                    }
                }
            }

            IndexStream itr = children.poll();

            // delayed nodes are not added to the union at this stage, lest they be duplicated once per active index stream
            pointers = pointers.union(itr.peek().second(), Collections.emptyList());
            itr.next();
            if (itr.hasNext()) {
                // add this to the processed list so other iterators have a chance to be first even if this one comes back earlier in the stack
                processedChildren.add(itr);
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("IndexStream exhausted for " + itr.getContextDebug());
                }
            }

            // repopulate children with all processed children
            if (children.isEmpty() && !processedChildren.isEmpty()) {
                children.addAll(processedChildren);
                processedChildren.clear();
            }
        }

        // build set of nodes, to include any delayed nodes in this union
        JexlNodeSet nodeSet = new JexlNodeSet();
        if (pointers.myNode != null)
            nodeSet.add(pointers.myNode);
        if (delayedNodes != null && !delayedNodes.isEmpty())
            nodeSet.addAll(delayedNodes);

        // rebuild current node
        currNode = null;
        if (nodeSet.size() == 1) {
            currNode = nodeSet.iterator().next();
        } else {
            currNode = JexlNodeFactory.createOrNode(nodeSet.getNodes());
        }

        // update pointers with the current node
        if (!delayedNodes.isEmpty()) {
            // pointers is composed of all live index streams, but we need to add in
            // any delayed nodes
            pointers.setNode(currNode);
        }

        return Tuples.tuple(dayOrShard, pointers);
    }

    private Tuple2<String,IndexInfo> buildHitFromDelayedTerms(String context) {
        JexlNode node;
        if (delayedNodes.size() == 1) {
            node = delayedNodes.getNodes().iterator().next();
        } else {
            node = JexlNodeFactory.createOrNode(delayedNodes.getNodes());
        }

        IndexInfo info = new IndexInfo(-1); // delayed nodes considered a shard range
        info.applyNode(node);

        return new Tuple2<>(context, info);
    }

    @Override
    public void remove() {}

    public static class Builder {
        protected boolean built = false;
        protected IdentityHashMap<BaseIndexStream,Object> children = new IdentityHashMap<>();
        protected List<ConcurrentScannerInitializer> todo = Lists.newArrayList();

        private Builder() {}

        public boolean addChild(BaseIndexStream child) {
            if (built) {
                throw new IllegalStateException("Builder already built an Union!");
            } else {
                return !children.containsKey(child) && children.put(child, null) == null;
            }
        }

        public int size() {
            return children.size() + todo.size();
        }

        public ArrayList<BaseIndexStream> children() {
            return Lists.newArrayList(children.keySet());
        }

        public void addChildren(List<ConcurrentScannerInitializer> todo) {
            this.todo.addAll(todo);
        }

        public Union build(ExecutorService service) {
            if (!todo.isEmpty()) {

                Collection<BaseIndexStream> streams = ConcurrentScannerInitializer.initializeScannerStreams(todo, service);
                for (BaseIndexStream stream : streams) {
                    addChild(stream);
                }
            }
            todo.clear();
            return new Union(children.keySet());
        }

        public void consume(Builder builder) {
            for (BaseIndexStream childStream : builder.children()) {
                addChild(childStream);
            }
            todo.addAll(builder.todo);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public StreamContext context() {
        return context;
    }

    @Override
    public String getContextDebug() {
        StringBuilder builder = new StringBuilder();
        builder.append(context).append(": Union (").append(contextDebug).append(')');
        for (String childrenContext : childrenContextDebug) {
            String prefix = "\n - ";
            String[] lines = StringUtils.split(childrenContext, '\n');
            for (String line : lines) {
                builder.append(prefix).append(line);
                prefix = "\n   ";
            }
        }
        return builder.toString();
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.query.index.lookup.IndexStream#currentNode()
     */
    @Override
    public JexlNode currentNode() {
        return currNode;
    }

    /**
     * Seek every IndexStream whose top shard is lower than the specified shard.
     *
     * @param seekShard
     *            the shard to seek to.
     * @return the lowest shard within this union after seeking
     */
    @Override
    public String seek(String seekShard) {

        // Guard against the case when a seek range like YYYYMMDD_ is provided.
        if (seekShard.charAt(seekShard.length() - 1) == '_')
            seekShard = new String(seekShard.getBytes(), 0, seekShard.length() - 1);

        // Check the current element first
        String topShard = isTopElementAMatch(seekShard);
        if (topShard != null) {
            // If the top element is a day and we are seeking to a shard range within the day, re-map the top element to the shard range.
            // This allows us to actually intersect.
            if (!isDay(seekShard) && ShardEquality.matches(this.next.first(), seekShard)) {
                this.next = new Tuple2<>(seekShard, this.next.second());
            }

            return topShard;
        } else {
            // If the top element did not match then null it out
            this.next = null;
        }

        List<IndexStream> nextChildren = new ArrayList<>();
        while (!children.isEmpty()) {

            IndexStream stream = children.poll();

            // If the top of the shard-sorted priority queue is beyond the specified shard then we can bail out.
            String streamDayOrShard = stream.peek().first();

            // Avoid seeking if the stream shard matches the seek shard, or if the stream shard is greater than the seek shard.
            if (ShardEquality.matches(streamDayOrShard, seekShard) || ShardEquality.greaterThanOrEqual(streamDayOrShard, seekShard)) {
                nextChildren.add(stream);
                continue;
            }

            // If the IndexStream has more elements after seeking, add it back into the priority queue.
            stream.seek(seekShard);
            if (stream.hasNext()) {
                nextChildren.add(stream);
            }
        }

        children.addAll(nextChildren);
        if (!children.isEmpty()) {
            next = advanceQueue(null);
            if (next != null) {
                return next.first();
            }
        }

        // If no child IndexStreams remain then this union is exhausted.
        return null;
    }

    // Is the top shard greater than or equal to the seek shard
    public String isTopElementAMatch(String seekShard) {
        // Check the current element first
        if (next != null) {

            String topShard = next.first();

            // If the top shard exceeds the seek shard then return the top shard
            if (ShardEquality.greaterThanOrEqual(topShard, seekShard)) {
                return topShard;
            }

            // If the top shard is a day range that matches the seek shard, return the seek shard.
            if (ShardEquality.matches(topShard, seekShard)) {
                // Return the existing maximum instead of the day
                return seekShard;
            }
        }
        return null;
    }
}
