package datawave.query.index.lookup;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.TreeMultimap;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.language.parser.jexl.JexlNodeSet;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuples;
import datawave.util.StringUtils;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.service.ServiceConfiguration;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

/**
 * Intersect global index range streams by the shard key denoting a day range or a shard range.
 *
 * Intersect global index range streams. This will take a set of underlying streams and will intersect them: e.g. stream 1: 20130102_4/UID1,UID2,UID3
 * 20130104_5/UID5 20130106_2/UID6 20130107/{inf} stream 2: 20130102_4/UID2,UID3 20130104_5/UID5 20130105_3/{inf} 20130106_2/{inf} 20130107_3/{inf}
 * 
 * result: 20130102_4/UID2,UID3 20130104_5/UID5 20130106_2/UID6 20130107_3/{inf}
 *
 * <code>
 * Assumptions:
 * 1) The streams are ordered
 * 2) Any one stream cannot contain a mix of day and shards for the same day (e.g. 20130101, 20130101_4, ...)
 *    In other words, each stream being intersected is a stream of days (20130101, 20130102 ...)
 *    or a stream of shards (20130101_4, 20130101_8, 20130102_3 ...).
 * </code>
 *
 * More formally, we are intersecting 2 to K iterators of 1 to N sorted elements. The iterators support seeking to the highest top key.
 *
 *
 * Some notes about base cases and edge cases.
 *
 * At a high level this class is concerned with intersecting a stream of keys that represent day or shard ranges. To see how individual hits are intersected see
 * {@link IndexInfo#intersect(IndexInfo)}.
 *
 * Only similar shards may be intersected (20190314_22 and 20190314_22), or shards that fall within a day range (20190314_22 and 20190314).
 *
 * This class implements an Iterator interface, as well as a PeekingIterator, and a seeking iterator. This functionality holds true at nearly all levels in the
 * iterator stack. As such it is imperative that care is taken when modifying code in this class or one of the other classes in this stack.
 *
 * <code>
 *     Simple Diagram of IndexStream stack
 * 
 *     Intersection ( you are here )
 *     - (1 or more layers of Unions, nested Unions and/or Intersections as described by the JexlNode tree)
 *     - Scanner Stream (1 per query term)
 *     - RangeStreamScanner (1 per ScannerStream)
 *     - Iterator on the global index (CreateUidsIterator, 1 per RangeStreamScanner)
 * </code>
 *
 * A note on Seeking to a shard. According to the method contract a seek call will return one of three values;
 * <ul>
 * <li>The shard specified.</li>
 * <li>The next-highest shard if the specified shard does not exist in the IndexStream.</li>
 * <li>Null if the specified shard lies beyond the bounds of this IndexStream.</li>
 * </ul>
 */
public class Intersection extends BaseIndexStream {
    
    private final StreamContext context;
    private final String contextDebug;
    
    private TreeMultimap<String,IndexStream> children;
    private final List<String> childrenContextDebug = new ArrayList<>();
    
    private final JexlNodeSet nodeSet = new JexlNodeSet(); // populated via active index streams
    private final JexlNodeSet delayedNodes = new JexlNodeSet(); // populated via delayed index streams
    
    private JexlNode currNode; // current query
    private Tuple2<String,IndexInfo> next;
    
    protected UidIntersector uidIntersector;
    private static final IndexStreamComparator streamComparator = new IndexStreamComparator();

    private ServiceConfiguration serviceConfiguration = ServiceConfiguration.getDefaultInstance();

    private static final Logger log = Logger.getLogger(Intersection.class);

    public Intersection(Collection<? extends IndexStream> streams, UidIntersector uidIntersector) {
        this(streams,uidIntersector,ServiceConfiguration.getDefaultInstance());
    }
    public Intersection(Collection<? extends IndexStream> streams, UidIntersector uidIntersector, ServiceConfiguration serviceConfiguration) {
        this.children = TreeMultimap.create(Ordering.natural(), streamComparator);
        this.uidIntersector = uidIntersector;
        this.serviceConfiguration=serviceConfiguration;

        if (log.isTraceEnabled()) {
            log.trace("Constructor -- has children? " + streams.isEmpty());
        }
        
        if (streams.isEmpty()) {
            this.context = StreamContext.ABSENT;
            this.contextDebug = "no children";
            return;
        }
        
        // flag tells us to short circuit
        boolean absent = false;
        
        for (IndexStream stream : streams) {
            if (log.isDebugEnabled()) {
                childrenContextDebug.add(stream.getContextDebug());
            }
            
            if (stream.hasNext()) {
                switch (stream.context()) {
                    case PRESENT:
                    case VARIABLE:
                    case EXCEEDED_VALUE_THRESHOLD:
                        if (log.isTraceEnabled()) {
                            log.trace("Stream has next, so adding it to children " + stream.peek().second().getNode() + " " + key(stream));
                        }
                        this.nodeSet.add(stream.currentNode());
                        this.children.put(key(stream), stream);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected stream context " + stream.context());
                }
            } else {
                switch (stream.context()) {
                    case ABSENT:
                    case EXCEEDED_VALUE_THRESHOLD:
                        // one or more index streams terminated early, no intersection possible
                        absent = true;
                        break;
                    case IGNORED:
                    case UNINDEXED:
                    case DELAYED_FIELD:
                    case UNKNOWN_FIELD:
                    case EXCEEDED_TERM_THRESHOLD:
                        this.delayedNodes.add(stream.currentNode());
                        break;
                    case NO_OP:
                        // this intersection is going to be merged with a parent intersection, do nothing
                        continue;
                    default:
                        QueryException qe = new QueryException(DatawaveErrorCode.EMPTY_RANGE_STREAM, MessageFormat.format("{0}", stream.context()));
                        throw new DatawaveFatalQueryException(qe);
                }
            }
        }
        if (log.isTraceEnabled())
            log.trace("size is " + this.children.size());
        
        JexlNodeSet allNodes = new JexlNodeSet();
        allNodes.addAll(nodeSet);
        allNodes.addAll(delayedNodes);
        
        currNode = JexlNodeFactory.createAndNode(allNodes);
        Preconditions.checkNotNull(currNode);
        
        // three cases 1) absent in which case no intersection exists, 2) some form of delayed, 3) valid intersect
        
        if (absent) {
            this.context = StreamContext.ABSENT;
            this.contextDebug = "found absent child";
        } else if (areAllChildrenSameContext(streams, StreamContext.DELAYED_FIELD)) {
            this.context = StreamContext.DELAYED_FIELD;
            this.contextDebug = "delayed field";
        } else if (areAllChildrenSameContext(streams, StreamContext.UNINDEXED)) {
            this.context = StreamContext.UNINDEXED;
            this.contextDebug = "all children unindexed";
        } else if (areAllChildrenSameContext(streams, StreamContext.IGNORED)) {
            this.context = StreamContext.IGNORED;
            this.contextDebug = "all children ignored";
        } else if (areAllChildrenSameContext(streams, StreamContext.EXCEEDED_TERM_THRESHOLD)) {
            this.context = StreamContext.EXCEEDED_TERM_THRESHOLD;
            this.contextDebug = "all children exceeded term threshold";
        } else if (areAllChildrenSameContext(streams, StreamContext.EXCEEDED_VALUE_THRESHOLD)) {
            this.context = StreamContext.EXCEEDED_VALUE_THRESHOLD;
            this.contextDebug = "all children exceeded value threshold";
            next(); // call next to populate the top key. this is a stream of days generated by the range stream
        } else if (this.children.isEmpty() && !delayedNodes.isEmpty()) {
            // we have a mix of delayed marker nodes
            this.context = StreamContext.DELAYED_FIELD;
            this.contextDebug = "children are a mix of delayed marker nodes";
        } else {
            // just because index streams are present, does not mean they actually intersect
            next();
            if (next != null) {
                if (delayedNodes.isEmpty()) {
                    this.context = StreamContext.PRESENT;
                    this.contextDebug = "children intersect";
                } else {
                    this.context = StreamContext.VARIABLE;
                    this.contextDebug = "children are a mix of delayed and non-delayed terms";
                }
            } else {
                this.context = StreamContext.ABSENT;
                this.contextDebug = "children did not intersect";
            }
        }
        
        if (log.isTraceEnabled())
            log.trace("Stream context " + context());
    }
    
    @Override
    public Tuple2<String,IndexInfo> peek() {
        return next;
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
                intersect(keys);
            } else {
                // Seek all child IndexStreams to the highest top key.
                String max = children.keySet().last();
                children = advanceStreams(children, max);
            }
        }
        return ret;
    }
    
    /**
     * Find the next result at or beyond the provided context.
     * <p>
     * Calls {@link #advanceStream(IndexStream, String)} before called {@link #next()}
     *
     * @param context
     *            a shard context
     * @return the result of calling {@link #next()}
     */
    @Override
    public Tuple2<String,IndexInfo> next(String context) {
        
        // advance all streams to the provided context
        children = advanceStreams(children, context);
        
        return next();
    }
    
    /**
     * Intersect the top elements of all child IndexStreams, then advance.
     *
     * @param keys
     *            a sorted set of child IndexStreams. Should be of size 1.
     */
    public void intersect(SortedSet<String> keys) {
        IndexInfo shard = intersect(children.values());
        
        if (shard.count() != 0) {
            next = Tuples.tuple(keys.first(), shard);
        }
        children = nextAll(keys.first(), children.get(keys.first()));
    }
    
    @Override
    public void remove() {}
    
    private static String key(PeekingIterator<Tuple2<String,IndexInfo>> itr) {
        return key(itr.peek());
    }
    
    public static String key(Tuple2<String,IndexInfo> tuple) {
        return tuple.first();
    }
    
    /**
     * Converts an iterator of String-IndexInfo to an iterator of IndexInfo objects
     *
     * @param i
     *            an iterator of iterators, irritating isn't it?
     * @return an iterator whose elements are IndexInfo objects
     */
    public static Iterable<IndexInfo> convert(Iterable<? extends PeekingIterator<Tuple2<String,IndexInfo>>> i) {
        final Function<PeekingIterator<Tuple2<String,IndexInfo>>,IndexInfo> f = itr -> {
            if (log.isTraceEnabled())
                log.trace("ah" + itr.peek().first() + " " + itr.peek().second());
            return itr.peek().second();
        };
        return Iterables.transform(i, f);
    }
    
    /**
     * Intersect the top {@link IndexInfo} elements.
     *
     * @param iterators
     *            an iterator of child IndexStreams
     * @return the top indexinfo elements
     */
    public IndexInfo intersect(Iterable<? extends PeekingIterator<Tuple2<String,IndexInfo>>> iterators) {
        Iterator<IndexInfo> infos = convert(iterators).iterator();
        IndexInfo merged = infos.next();
        
        nodeSet.clear();
        nodeSet.add(merged.getNode());
        
        boolean childrenAdded = false;
        
        while (infos.hasNext()) {
            
            IndexInfo next = infos.next();
            
            nodeSet.add(next.getNode());
            merged = merged.intersect(next, Collections.emptyList(), uidIntersector, serviceConfiguration);
            childrenAdded = true;
        }
        
        // add in delayed nodes after we're done intersecting the viable index streams
        if (!childrenAdded || !delayedNodes.isEmpty()) {
            childrenAdded = merged.intersect(Lists.newArrayList(delayedNodes.getNodes()));
        }
        
        if (log.isTraceEnabled())
            log.trace("intersect " + childrenAdded);
        
        // Add all delayed nodes back into the nodeSet
        nodeSet.addAll(delayedNodes);
        currNode = JexlNodeFactory.createAndNode(nodeSet.getNodes());
        
        if (!childrenAdded) {
            if (log.isTraceEnabled()) {
                log.trace("can't add children");
            }
            merged.setNode(currNode);
        }
        return merged;
    }
    
    /**
     * Checks all child index streams in this intersection against the provided context
     *
     * @param streams
     *            a collection of IndexStream
     * @param context
     *            a {@link datawave.query.index.lookup.IndexStream.StreamContext}
     * @return true if all child index streams have the specified context
     */
    public static boolean areAllChildrenSameContext(Collection<? extends IndexStream> streams, StreamContext context) {
        if (!streams.isEmpty()) {
            for (IndexStream stream : streams) {
                if (!stream.context().equals(context)) {
                    return false;
                }
            }
            return true;
        }
        return false; // no index streams
    }
    
    /*
     * This method will advance all streams except for those returning a day. However, if the key is a day then they are all advanced anyway. The reason for the
     * special handling of "day" ranges is that multiple shards from a separate stream may match the day range and we need to ensure all of them get a chance.
     * If the key is a "day" range, then all streams matched that day so we can safely advance them all. If the iterator `hasNext` after that, then it is added
     * into the returned multimap with the next key it will return. If an iterator ever does not have a next, an empty multimap is returned, signifying the
     * exhaustion of this intersection.
     */
    public static TreeMultimap<String,IndexStream> nextAll(String key, Collection<IndexStream> streams) {
        TreeMultimap<String,IndexStream> newChildren = TreeMultimap.create(Ordering.natural(), streamComparator);
        for (IndexStream itr : streams) {
            
            // If we are next'ing based on a shard and this is a day, keep the day.
            if (!ShardEquality.isDay(key) && ShardEquality.isDay(key(itr.peek()))) {
                newChildren.put(itr.peek().first(), itr);
            } else {
                
                //
                String context = !newChildren.keySet().isEmpty() ? newChildren.keySet().first() : key;
                itr.next(context);
                
                if (itr.hasNext()) {
                    newChildren.put(itr.peek().first(), itr);
                } else {
                    return TreeMultimap.create(Ordering.natural(), streamComparator);
                }
            }
        }
        return newChildren;
    }
    
    /*
     * This method is deprecated because it is faster to seek() the underlying RangeStreamScanner than to next() through the IndexStream.
     * 
     * Consider the arbitrary case of intersecting two streams of integers. One stream contains every integer between 1 and 100, inclusive. The second stream
     * has two values, 1 and 100. There is no point in advancing the stream with a hundred integers by calling next() 99 times when a single seek() call would
     * suffice.
     * 
     * 
     * Calls `next()` on all iterators that aren't mapped to the highest key in the multimap until that iterator's next value (as returned by `peek()`) is
     * greater than or equal to the previous max key.
     * 
     * If an iterator gets exhausted, then an empty multimap is returned signifying the end of this intersection.
     * 
     * <code>children</code> must be non-empty
     */
    @Deprecated
    public static TreeMultimap<String,IndexStream> pivot(TreeMultimap<String,IndexStream> children) {
        TreeMultimap<String,IndexStream> newChildren = TreeMultimap.create(Ordering.natural(), streamComparator);
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
                if (ShardEquality.isDay(dayOrShard) && max.startsWith(dayOrShard)) {
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
                return TreeMultimap.create(Ordering.natural(), streamComparator);
            }
        }
        return newChildren;
    }
    
    /**
     * For each IndexStream not already mapped to the high key, advance the stream to the specified high key. This method will dynamically update the high key
     * if a new high key is found.
     *
     * If an IndexStream is exhausted by the seek call, an empty multi-map is returned to signify the end of this intersection.
     *
     * @param children
     *            a non-empty sorted multi-map of {@link IndexStream}
     * @param max
     *            the seek key
     * @return a new sorted multi-map of {@link IndexStream}, or an empty multi-map if the intersection is exhausted
     */
    public static TreeMultimap<String,IndexStream> advanceStreams(TreeMultimap<String,IndexStream> children, String max) {
        TreeMultimap<String,IndexStream> newChildren = TreeMultimap.create(Ordering.natural(), streamComparator);
        
        // Remove all IndexStreams already mapped to the highest key, if they exist
        if (children.containsKey(max)) {
            newChildren.putAll(max, children.removeAll(max));
        }
        
        for (IndexStream stream : children.values()) {
            
            // Advance the IndexStream to the high key.
            String dayOrShard = advanceStream(stream, max);
            
            // Cannot intersect with an empty IndexStream, return empty multimap to signify end of intersection.
            if (dayOrShard == null || !stream.hasNext()) {
                return TreeMultimap.create(Ordering.natural(), streamComparator);
            } else {
                // add the item into our map
                newChildren.put(dayOrShard, stream);
                
                // Adaptive max.
                if (dayOrShard.compareTo(max) > 0) {
                    max = dayOrShard;
                }
            }
        }
        return newChildren;
    }
    
    /**
     * Advance the IndexStream to the high key (shard), or the next-highest key if the high key does not exist but there are still results.
     *
     * If no results exist at or beyond the high key, a null value is returned.
     *
     * @param stream
     *            the IndexStream
     * @param max
     *            the seek key
     * @return the shard advanced to, or null if the IndexStream is exhausted
     */
    public static String advanceStream(IndexStream stream, String max) {
        String dayOrShard = null;
        while (stream.hasNext()) {
            
            dayOrShard = stream.peek().first();
            
            if (ShardEquality.greaterThanOrEqual(dayOrShard, max)) {
                break;
            }
            if (ShardEquality.matches(dayOrShard, max)) {
                dayOrShard = max;
                break;
            }
            
            // Delegate seek operation to the underlying IndexStream
            dayOrShard = stream.seek(max);
        }
        return dayOrShard;
    }
    
    /**
     * Seek the underlying IndexStreams to the specified high key.
     *
     * This method is a wrapper around {@link #advanceStreams}. It ensures this intersection always seeks to the next common key.
     *
     * @param seekShard
     *            the shard to advance to.
     * @return the lowest shard after seeking
     */
    @Override
    public String seek(String seekShard) {
        
        // Guard against the case when an seek range like YYYYMMDD_ is provided.
        if (seekShard.charAt(seekShard.length() - 1) == '_')
            seekShard = new String(seekShard.getBytes(), 0, seekShard.length() - 1);
        
        // Check the current element first
        String currShard = isTopElementAMatch(seekShard);
        if (currShard != null) {
            // If the top element is a day and we are seeking to a shard range within the day, re-map the top element to the shard range. This allows us to
            // actually intersect.
            if (ShardEquality.matches(this.next.first(), seekShard) && ShardEquality.isShard(seekShard)) {
                this.next = new Tuple2<>(seekShard, this.next.second());
            }
            
            return currShard;
        } else {
            // If the top element did not match then null it out. We're skipping forward.
            this.next = null;
        }
        
        // Initial seek
        children = advanceStreams(children, seekShard);
        
        // Continue seeking until the next intersection is reached (when each IndexStream maps to a single common key)
        while (children.keySet().size() > 1) {
            String max = children.keySet().last();
            children = advanceStreams(children, max);
        }
        
        if (!children.isEmpty()) {
            // We only advanced the IndexStreams. Now intersect them and return the shard key for this intersection.
            next();
            
            // Check to make sure this intersection did not advance beyond the end of it's data.
            if (next != null) {
                return next.first();
            }
        }
        return null;
    }
    
    /**
     * Used to determine if this intersection should be seek'd to the provided shard.
     *
     * Returns false if the top element sorts less than the provided shard.
     *
     * @param shard
     *            the shard to match
     * @return true if the top element is equal or greater than the shard
     */
    public String isTopElementAMatch(String shard) {
        // Check the current element first
        if (next != null) {
            
            // Is it greater than or equal to the seek shard
            String topShard = next.first();
            
            if (ShardEquality.greaterThanOrEqual(topShard, shard)) {
                return topShard;
            }
            if (ShardEquality.matches(topShard, shard)) {
                return shard;
            }
        }
        return null;
    }
    
    public static class Builder {
        protected boolean built = false;
        
        protected UidIntersector uidIntersector = new IndexInfo();
        
        protected IdentityHashMap<BaseIndexStream,Object> children = new IdentityHashMap<>();
        
        protected List<ConcurrentScannerInitializer> todo = Lists.newArrayList();
        
        private Builder() {}
        
        public void setUidIntersector(UidIntersector uidIntersector) {
            this.uidIntersector = uidIntersector;
        }
        
        public boolean addChild(BaseIndexStream child) {
            if (built) {
                throw new IllegalStateException("Builder already built an Intersection!");
            } else {
                return children.containsKey(child) ? false : children.put(child, null) == null;
            }
        }
        
        public ArrayList<BaseIndexStream> children() {
            return Lists.newArrayList(children.keySet());
        }

        public Intersection build(ExecutorService service, ShardQueryConfiguration queryConfiguration) {
            
            if (!todo.isEmpty()) {
                if (log.isTraceEnabled())
                    log.trace("building " + todo.size() + " scanners concurrently");
                Collection<BaseIndexStream> streams = ConcurrentScannerInitializer.initializeScannerStreams(todo, service);
                for (BaseIndexStream stream : streams) {
                    addChild(stream);
                }
            }
            todo.clear();
            built = true;
            return new Intersection(children.keySet(), uidIntersector, queryConfiguration.getServiceConfiguration());
        }
        
        public void addChildren(List<ConcurrentScannerInitializer> todo) {
            this.todo.addAll(todo);
        }
        
        public void consume(Builder builder) {
            for (BaseIndexStream childStream : builder.children()) {
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
