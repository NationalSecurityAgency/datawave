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

/**
 * Creates a union of global index range streams.
 */
public class Union implements IndexStream {
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
            if (itr.hasNext())
                children.add(itr);
            childrenAdded = true;
        }
        
        JexlNodeSet nodeSet = new JexlNodeSet();
        if (pointers.myNode != null)
            nodeSet.add(pointers.myNode);
        if (delayedNodes != null && !delayedNodes.isEmpty())
            nodeSet.directAddAll(delayedNodes);
        
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
    
    public static boolean isDay(String dayOrShard) {
        return (dayOrShard.indexOf('_') < 0);
    }
    
    public static boolean isShard(String dayOrShard) {
        return !isDay(dayOrShard);
    }
    
    @Override
    public void remove() {}
    
    public static class Builder {
        protected boolean built = false;
        protected IdentityHashMap<IndexStream,Object> children = new IdentityHashMap<>();
        protected List<ConcurrentScannerInitializer> todo = Lists.newArrayList();
        
        private Builder() {}
        
        public boolean addChild(IndexStream child) {
            if (built) {
                throw new IllegalStateException("Builder already built an Union!");
            } else {
                return !children.containsKey(child) && children.put(child, null) == null;
            }
        }
        
        public int size() {
            return children.size() + todo.size();
        }
        
        public ArrayList<IndexStream> children() {
            return Lists.newArrayList(children.keySet());
        }
        
        public void addChildren(List<ConcurrentScannerInitializer> todo) {
            this.todo.addAll(todo);
            
        }
        
        public Union build(ExecutorService service) {
            if (!todo.isEmpty()) {
                
                Collection<IndexStream> streams = ConcurrentScannerInitializer.initializeScannerStreams(todo, service);
                for (IndexStream stream : streams) {
                    addChild(stream);
                }
            }
            todo.clear();
            return new Union(children.keySet());
        }
        
        public void consume(Builder builder) {
            for (IndexStream childStream : builder.children()) {
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
}
