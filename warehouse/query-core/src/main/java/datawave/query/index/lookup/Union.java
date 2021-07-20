package datawave.query.index.lookup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;

import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.language.parser.jexl.JexlNodeSet;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;

import datawave.util.StringUtils;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import static datawave.query.index.lookup.ShardEquality.isDay;

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
    public Union(Iterable<? extends IndexStream> children) {
        this.children = new PriorityQueue(16, PeekOrdering.make(new TupleComparator<>()));
        this.childNodes = new JexlNodeSet();
        this.delayedNodes = new JexlNodeSet();
        int childrenCount = 0;
        boolean childrenIgnored = false;
        boolean unindexedField = false;
        boolean delayedField = false;
        boolean exceededTermThreshold = false;
        boolean exceededValueThreshold = false;
        for (IndexStream stream : children) {
            
            if (log.isDebugEnabled()) {
                childrenContextDebug.add(stream.getContextDebug());
            }
            
            childrenCount++;
            if (log.isTraceEnabled()) {
                log.trace("Union of " + stream.currentNode() + " " + stream.hasNext() + " " + JexlStringBuildingVisitor.buildQuery(stream.currentNode()));
                log.trace("Union of " + stream + " " + stream.context());
            }
            
            if (StreamContext.NO_OP == stream.context())
                continue;
            if (StreamContext.EXCEEDED_VALUE_THRESHOLD == stream.context()) {
                exceededValueThreshold = true;
            } else if (StreamContext.EXCEEDED_TERM_THRESHOLD == stream.context()) {
                exceededTermThreshold = true;
            } else if (StreamContext.UNINDEXED == stream.context()) {
                this.childNodes.add(stream.currentNode());
                this.delayedNodes.add(stream.currentNode());
                unindexedField = true;
                continue;
            } else if (StreamContext.DELAYED_FIELD == stream.context()) {
                this.childNodes.add(stream.currentNode());
                this.delayedNodes.add(stream.currentNode());
                delayedField = true;
                continue;
            }
            
            if (stream.hasNext()) {
                this.children.add(stream);
                this.childNodes.add(stream.currentNode());
            } else {
                switch (stream.context()) {
                
                    case INITIALIZED:
                        // this should never be returned
                        throw new RuntimeException("Invalid state in RangeStream");
                    case IGNORED:
                        childrenIgnored = true;
                        this.childNodes.add(stream.currentNode());
                        this.delayedNodes.add(stream.currentNode());
                        break;
                    case EXCEEDED_VALUE_THRESHOLD:
                    case EXCEEDED_TERM_THRESHOLD:
                        if (log.isTraceEnabled())
                            log.trace("Adding current node to stream");
                        /*
                         * Helpful for debugging
                         */
                        this.childNodes.add(stream.currentNode());
                        break;
                    case VARIABLE:
                    case UNKNOWN_FIELD:
                        this.delayedNodes.add(stream.currentNode());
                    case ABSENT:
                    case PRESENT:
                    default:
                        break;
                }
            }
        }
        if (log.isTraceEnabled())
            log.trace("children count is " + childrenCount + " " + childNodes.size() + " " + this.children.size());
        if (childNodes.size() == 1)
            currNode = JexlNodeFactory.createUnwrappedOrNode(this.childNodes.getNodes());
        else
            currNode = JexlNodeFactory.createOrNode(this.childNodes.getNodes());
        
        if (this.children.isEmpty()) {
            if (childrenIgnored || childrenCount == 0) {
                this.context = StreamContext.IGNORED;
                this.contextDebug = "no children";
            } else if (unindexedField) {
                this.context = StreamContext.UNINDEXED;
                this.contextDebug = "unindexed child";
            } else if (delayedField) {
                this.context = StreamContext.DELAYED_FIELD;
                this.contextDebug = "delayed child";
            } else {
                this.context = StreamContext.ABSENT;
                this.contextDebug = "no children";
            }
        } else {
            if (exceededTermThreshold) {
                this.context = StreamContext.EXCEEDED_TERM_THRESHOLD;
                this.contextDebug = "ExceededTermThreshold child";
            } else if (exceededValueThreshold) {
                this.context = StreamContext.EXCEEDED_VALUE_THRESHOLD;
                this.contextDebug = "ExceededValueThreshold child";
            } else if (unindexedField) {
                this.context = StreamContext.UNINDEXED;
                this.contextDebug = "Unindexed child";
            } else {
                this.context = StreamContext.PRESENT;
                this.contextDebug = "children are present";
            }
            next();
        }
    }
    
    @Override
    public boolean hasNext() {
        return next != null;
    }
    
    @Override
    public Tuple2<String,IndexInfo> next() {
        Tuple2<String,IndexInfo> ret = next;
        next = advanceQueue();
        return ret;
    }
    
    @Override
    public Tuple2<String,IndexInfo> peek() {
        return next;
    }
    
    private Tuple2<String,IndexInfo> advanceQueue() {
        if (children.isEmpty()) {
            return null;
        }
        Tuple2<String,IndexInfo> head = children.peek().peek();
        // shard can be either a specific shard or a day denoting all shards
        String dayOrShard = head.first();
        IndexInfo pointers = head.second();
        // use startsWith to match shards with a day
        if (log.isTraceEnabled())
            log.trace("advancing " + pointers.getNode() + " " + children.peek().context());
        
        boolean childrenAdded = false;
        while (!children.isEmpty()) {
            String streamDayOrShard = children.peek().peek().first();
            if (log.isTraceEnabled())
                log.trace("we have " + streamDayOrShard + " " + dayOrShard);
            
            if (!streamDayOrShard.equals(dayOrShard)) {
                // additional test if dayOrShard is a day
                if (!(isDay(dayOrShard) && streamDayOrShard.startsWith(dayOrShard))) {
                    break;
                }
            }
            
            IndexStream itr = children.poll();
            
            pointers = pointers.union(itr.peek().second(), Lists.newArrayList(delayedNodes.getNodes()));
            itr.next();
            if (itr.hasNext()) {
                children.add(itr);
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("IndexStream exhausted for " + itr.getContextDebug());
                }
            }
            childrenAdded = true;
        }
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        if (pointers.myNode != null)
            nodeSet.add(pointers.myNode);
        if (delayedNodes != null && !delayedNodes.isEmpty())
            nodeSet.addAll(delayedNodes);
        
        currNode = null;
        if (nodeSet.size() == 1) {
            currNode = nodeSet.iterator().next();
        } else {
            currNode = JexlNodeFactory.createUnwrappedOrNode(nodeSet.getNodes());
        }
        
        if (!childrenAdded) {
            pointers.setNode(currNode);
        }
        
        return Tuples.tuple(dayOrShard, pointers);
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
            next = advanceQueue();
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
