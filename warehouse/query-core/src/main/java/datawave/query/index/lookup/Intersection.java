package datawave.query.index.lookup;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;
import datawave.util.StringUtils;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

/**
 * Intersect global index range streams. This will take a set of underlying streams and will intersect them: e.g. stream 1: 20130102_4/UID1,UID2,UID3
 * 20130104_5/UID5 20130106_2/UID6 20130107/{inf} stream 2: 20130102_4/UID2,UID3 20130104_5/UID5 20130105_3/{inf} 20130106_2/{inf} 20130107_3/{inf}
 * 
 * result: 20130102_4/UID2,UID3 20130104_5/UID5 20130106_2/UID6 20130107_3/{inf}
 * 
 * Assumptions: 1) The streams are ordered 2) Any one stream cannot contain a mix of day and shards for the same day (e.g. 20130101, 20130101_4, ...) In other
 * words, each stream being intersected is a stream of days (20130101, 20130102 ...) or a stream of shards (20130101_4, 20130101_8, 20130102_3 ...).
 */
public class Intersection implements IndexStream {
    private TreeMultimap<String,IndexStream> children;
    private final StreamContext context;
    private final String contextDebug;
    private final List<String> childrenContextDebug = new ArrayList<String>();
    Multimap<String,JexlNode> nodesMap = ArrayListMultimap.create();
    private Tuple2<String,IndexInfo> next;
    private JexlNode currNode;
    protected List<JexlNode> delayedNodes;
    protected boolean isVariable = false;
    protected UidIntersector uidIntersector;
    
    private static final Logger log = Logger.getLogger(Intersection.class);
    
    public Intersection(Iterable<? extends IndexStream> children, UidIntersector uidIntersector) {
        this.children = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());
        this.uidIntersector = uidIntersector;
        delayedNodes = Lists.newArrayList();
        Iterator<? extends IndexStream> childrenItr = children.iterator();
        
        boolean allExceededValueThreshold = true;
        
        if (log.isTraceEnabled()) {
            log.trace("Constructor -- has children? " + childrenItr.hasNext());
        }
        isVariable = false;
        if (childrenItr.hasNext()) {
            boolean absent = false;
            boolean delayedField = false;
            while (childrenItr.hasNext()) {
                IndexStream stream = childrenItr.next();
                if (log.isDebugEnabled()) {
                    childrenContextDebug.add(stream.getContextDebug());
                }
                
                if (StreamContext.NO_OP == stream.context())
                    continue;
                
                boolean exceededValueThreshold = false;
                
                if (stream.hasNext()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Stream has next, so adding it to children " + stream.peek().second().getNode() + " " + key(stream));
                    }
                    
                    if (StreamContext.VARIABLE == stream.context()) {
                        if (log.isTraceEnabled())
                            log.trace("Setting variable nodes");
                        isVariable = true;
                        JexlNode node = stream.peek().second().getNode();
                        nodesMap.put(JexlStringBuildingVisitor.buildQueryWithoutParse(node), node);
                        this.children.put(key(stream), stream);
                    } else {
                        
                        if (StreamContext.EXCEEDED_VALUE_THRESHOLD == stream.context())
                            exceededValueThreshold = true;
                        
                        nodesMap.put(JexlStringBuildingVisitor.buildQueryWithoutParse(stream.currentNode()), stream.currentNode());
                        this.children.put(key(stream), stream);
                    }
                } else {
                    if (StreamContext.EXCEEDED_TERM_THRESHOLD == stream.context()) {
                        delayedNodes.add(stream.currentNode());
                    } else if (StreamContext.EXCEEDED_VALUE_THRESHOLD == stream.context()) {
                        exceededValueThreshold = true;
                        absent = true;
                    } else if (StreamContext.ABSENT == stream.context()) {
                        absent = true;
                    } else if (StreamContext.UNINDEXED == stream.context() || StreamContext.UNKNOWN_FIELD == stream.context()
                                    || StreamContext.DELAYED_FIELD == stream.context() || StreamContext.IGNORED == stream.context()) {
                        if (StreamContext.DELAYED_FIELD == stream.context())
                            delayedField = true;
                        delayedNodes.add(stream.currentNode());
                        nodesMap.put(JexlStringBuildingVisitor.buildQueryWithoutParse(stream.currentNode()), stream.currentNode());
                    } else {
                        QueryException qe = new QueryException(DatawaveErrorCode.EMPTY_RANGE_STREAM, MessageFormat.format("{0}", stream.context()));
                        throw new DatawaveFatalQueryException(qe);
                    }
                }
                
                if (!exceededValueThreshold)
                    allExceededValueThreshold = false;
            }
            if (log.isTraceEnabled())
                log.trace("size is " + this.children.size());
            
            currNode = buildCurrentNode();
            
            Preconditions.checkNotNull(currNode);
            
            if (absent) {
                this.context = StreamContext.ABSENT;
                this.contextDebug = "found absent child";
            } else if (allChildrenAreUnindexed(this.children.values())) {
                this.context = StreamContext.UNINDEXED;
                this.contextDebug = "all children unindexed";
            } else if (this.children.size() == 0 && delayedField) {
                this.context = StreamContext.DELAYED_FIELD;
                this.contextDebug = "delayed field";
            } else if (allExceededValueThreshold) {
                this.context = StreamContext.EXCEEDED_VALUE_THRESHOLD;
                this.contextDebug = "all children exceeded value threshold";
                next();
            } else {
                this.context = StreamContext.PRESENT;
                this.contextDebug = "children may intersect";
                next();
            }
            
        } else {
            this.context = StreamContext.ABSENT;
            this.contextDebug = "no children";
        }
        if (log.isTraceEnabled())
            log.trace("Stream context " + this.context);
        
    }
    
    private JexlNode buildCurrentNode() {
        Set<JexlNode> allNodes = Sets.newHashSet();
        for (JexlNode node : delayedNodes) {
            nodesMap.put(JexlStringBuildingVisitor.buildQueryWithoutParse(node), node);
        }
        
        for (String key : nodesMap.keySet()) {
            Collection<JexlNode> nodeColl = nodesMap.get(key);
            JexlNode delayedNode = null;
            if (nodeColl.size() > 1) {
                log.trace(key + " has more than one node. taking first");
                for (JexlNode node : nodeColl) {
                    if (isDelayed(node)) {
                        delayedNode = node;
                        break;
                    }
                }
            }
            if (null != delayedNode)
                allNodes.add(delayedNode);
            else
                allNodes.add(nodeColl.iterator().next());
        }
        return JexlNodeFactory.createAndNode(FluentIterable.from(allNodes).filter(Predicates.notNull()).toList());
        
    }
    
    protected boolean isDelayed(JexlNode testNode) {
        if (ASTDelayedPredicate.instanceOf(testNode)) {
            return true;
            // We do not consider the index hole marker delayed as that is only a hole in the global index, not the field index
            // } else if (IndexHoleMarkerJexlNode.instanceOf(testNode)) {
            // return true;
        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(testNode)) {
            return true;
        } else if (ExceededTermThresholdMarkerJexlNode.instanceOf(testNode)) {
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public boolean hasNext() {
        return next != null;
    }
    
    @Override
    public Tuple2<String,IndexInfo> next() {
        Tuple2<String,IndexInfo> ret = next;
        
        next = null;
        
        while (!children.isEmpty() && next == null) {
            final SortedSet<String> keys = children.keySet();
            if (keys.size() == 1) {
                IndexInfo shard = intersect(children.values());
                
                if (shard.count() != 0) {
                    next = Tuples.tuple(keys.first(), shard);
                }
                children = nextAll(keys.first(), children.get(keys.first()));
            } else {
                
                children = pivot(children);
            }
        }
        
        return ret;
    }
    
    @Override
    public Tuple2<String,IndexInfo> peek() {
        return next;
    }
    
    @Override
    public void remove() {}
    
    private static String key(PeekingIterator<Tuple2<String,IndexInfo>> itr) {
        return key(itr.peek());
    }
    
    static String key(Tuple2<String,IndexInfo> tuple) {
        return tuple.first();
    }
    
    static Iterable<IndexInfo> convert(Iterable<? extends PeekingIterator<Tuple2<String,IndexInfo>>> i) {
        final Function<PeekingIterator<Tuple2<String,IndexInfo>>,IndexInfo> f = itr -> {
            if (log.isTraceEnabled())
                log.trace("ah" + itr.peek().first() + " " + itr.peek().second());
            return itr.peek().second();
        };
        return Iterables.transform(i, f);
    }
    
    IndexInfo intersect(Iterable<? extends PeekingIterator<Tuple2<String,IndexInfo>>> iterators) {
        Iterator<IndexInfo> infos = convert(iterators).iterator();
        IndexInfo merged = infos.next();
        
        nodesMap = ArrayListMultimap.create();
        
        nodesMap.put(JexlStringBuildingVisitor.buildQueryWithoutParse(merged.getNode()), merged.getNode());
        
        boolean childrenAdded = false;
        
        while (infos.hasNext()) {
            
            IndexInfo next = infos.next();
            
            nodesMap.put(JexlStringBuildingVisitor.buildQueryWithoutParse(next.getNode()), next.getNode());
            merged = merged.intersect(next, delayedNodes, uidIntersector);
            childrenAdded = true;
        }
        
        if (log.isTraceEnabled())
            log.trace("intserct " + childrenAdded);
        
        for (JexlNode node : delayedNodes) {
            nodesMap.put(JexlStringBuildingVisitor.buildQueryWithoutParse(node), node);
        }
        
        currNode = buildCurrentNode();
        
        if (!childrenAdded) {
            if (delayedNodes.size() > 0)
                childrenAdded = merged.intersect(delayedNodes);
            
        }
        
        if (!childrenAdded) {
            log.trace("can't add children");
            merged.setNode(currNode);
        }
        
        return merged;
    }
    
    static boolean allChildrenAreUnindexed(Iterable<IndexStream> stuff) {
        for (IndexStream is : stuff) {
            if (StreamContext.UNINDEXED != is.context()) {
                return false;
            }
        }
        return true;
    }
    
    /*
     * This method will advance all of the streams except for those returning a day. However if the key is a day then they are all advanced anyway. The reason
     * for the special handling of "day" ranges is that multiple shards from a separate stream may match the day range and we need to ensure all of them get a
     * chance. If the key is a "day" range, then all of the streams matched that day so we can safely advance them all. If the iterator `hasNext` after that,
     * then it is added into the returned multimap with the next key it will return. If an iterator ever does not have a have a next, an empty multimap is
     * returned, singifiying the exhaustion of this intersection.
     */
    static TreeMultimap<String,IndexStream> nextAll(String key, Collection<IndexStream> streams) {
        TreeMultimap<String,IndexStream> newChildren = TreeMultimap.create(Ordering.<String> natural(), Ordering.arbitrary());
        for (IndexStream itr : streams) {
            if (!isDay(key) && isDay(key(itr.peek()))) {
                newChildren.put(itr.peek().first(), itr);
            } else {
                itr.next();
                if (itr.hasNext()) {
                    newChildren.put(itr.peek().first(), itr);
                } else {
                    return TreeMultimap.create(Ordering.<String> natural(), Ordering.arbitrary());
                }
            }
        }
        return newChildren;
    }
    
    /*
     * Calls `next()` on all iterators that aren't mapped to the highest key in the multimap until that iterator's next value (as returned by `peek()`) is
     * greater than or equal to the previous max key.
     * 
     * If an iterator gets exhausted, then an empty multimap is returned singifying the end of this intersection.
     * 
     * <code>children</code> must be non-empty
     */
    static TreeMultimap<String,IndexStream> pivot(TreeMultimap<String,IndexStream> children) {
        TreeMultimap<String,IndexStream> newChildren = TreeMultimap.create(Ordering.<String> natural(), Ordering.arbitrary());
        final String max = children.keySet().last();
        newChildren.putAll(max, children.removeAll(max));
        for (IndexStream itr : children.values()) {
            // move this iterator forward until we get to a key past the max processed thus far
            String dayOrShard = null;
            while (itr.hasNext()) {
                
                dayOrShard = key(itr.peek());
                if (dayOrShard.compareTo(max) >= 0) {
                    break;
                }
                if (isDay(dayOrShard) && max.startsWith(dayOrShard)) {
                    // use the existing max instead of the day to add to the list
                    dayOrShard = max;
                    break;
                }
                itr.next();
            }
            // add the item into our map
            if (itr.hasNext()) {
                newChildren.put(dayOrShard, itr);
            } else {
                // nobody has anything past max, so no intersection
                return TreeMultimap.create(Ordering.<String> natural(), Ordering.arbitrary());
            }
        }
        return newChildren;
    }
    
    static boolean isDay(String dayOrShard) {
        return (dayOrShard.indexOf('_') < 0);
    }
    
    static boolean isShard(String dayOrShard) {
        return !isDay(dayOrShard);
    }
    
    public static class Builder {
        protected boolean built = false;
        
        protected UidIntersector uidIntersector = new IndexInfo();
        
        protected IdentityHashMap<IndexStream,Object> children = new IdentityHashMap<IndexStream,Object>();
        
        protected List<ConcurrentScannerInitializer> todo = Lists.newArrayList();
        
        private Builder() {}
        
        public void setUidIntersector(UidIntersector uidIntersector) {
            this.uidIntersector = uidIntersector;
        }
        
        public boolean addChild(IndexStream child) {
            if (built) {
                throw new IllegalStateException("Builder already built an Intersection!");
            } else {
                return children.containsKey(child) ? false : children.put(child, null) == null;
            }
        }
        
        public ArrayList<IndexStream> children() {
            return Lists.newArrayList(children.keySet());
        }
        
        public Intersection build(ExecutorService service) {
            
            if (todo.size() > 0) {
                if (log.isTraceEnabled())
                    log.trace("building " + todo.size() + " scanners concurrently");
                Collection<IndexStream> streams = ConcurrentScannerInitializer.initializeScannerStreams(todo, service);
                for (IndexStream stream : streams) {
                    addChild(stream);
                }
            }
            todo.clear();
            built = true;
            return new Intersection(children.keySet(), uidIntersector);
        }
        
        public void addChildren(List<ConcurrentScannerInitializer> todo) {
            this.todo.addAll(todo);
            
        }
        
        public void consume(Builder builder) {
            for (IndexStream childStream : builder.children()) {
                addChild(childStream);
            }
            todo.addAll(builder.todo);
        }
        
        public int size() {
            return todo.size() + children.size();
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
        builder.append(context).append(": Intersection (").append(contextDebug).append(')');
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
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (null != currNode)
            builder.append(JexlStringBuildingVisitor.buildQuery(currNode));
        return builder.toString();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.query.index.lookup.IndexStream#currentNode()
     */
    @Override
    public JexlNode currentNode() {
        if (log.isTraceEnabled()) {
            log.trace("we have " + JexlStringBuildingVisitor.buildQuery(currNode));
            if (null != next) {
                log.trace(" " + next.first());
            }
        }
        return currNode;
    }
}
