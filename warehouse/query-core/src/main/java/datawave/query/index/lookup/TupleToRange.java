package datawave.query.index.lookup;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import datawave.query.rewrite.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.rewrite.planner.QueryPlan;
import datawave.query.util.Tuple2;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class TupleToRange implements Function<Tuple2<String,IndexInfo>,Iterator<QueryPlan>> {
    
    public static final String NULL_BYTE_STRING = "\u0000";
    public static final String MAX_UNICODE_STRING = new String(Character.toChars(Character.MAX_CODE_POINT));
    
    private static final Logger log = Logger.getLogger(TupleToRange.class);
    protected JexlNode currentScript = null;
    protected JexlNode tree = null;
    protected RefactoredShardQueryConfiguration config = null;
    
    /**
     * @param currentNode
     * @param config
     */
    public TupleToRange(JexlNode currentNode, RefactoredShardQueryConfiguration config) {
        this.currentScript = currentNode;
        this.config = config;
        
    }
    
    public Iterator<QueryPlan> apply(Tuple2<String,IndexInfo> tuple) {
        IndexInfo ii = tuple.second();
        
        JexlNode queryNode = currentScript;
        if (ii.getNode() != null) {
            if (log.isTraceEnabled()) {
                log.trace("Got it from tuple " + JexlStringBuildingVisitor.buildQuery(ii.getNode()));
            }
            
        }
        
        if (ii.uids().size() > 0) {
            List<QueryPlan> ranges = Lists.newArrayListWithCapacity(ii.uids().size());
            for (IndexMatch uid : ii.uids()) {
                Key start = new Key(tuple.first(), uid.getUid());
                Key end = start.followingKey(PartialKey.ROW_COLFAM);
                
                if (config.isTldQuery()) {
                    end = new Key(tuple.first(), uid.getUid() + MAX_UNICODE_STRING);
                }
                // Technically, we don't want to be inclusive of the start key,
                // however if we mark the startKey as non-inclusive, when we create
                // the fi\x00 range in IndexIterator, we lost the context of "do we
                // want a single event" or "did we get restarted and this is the last
                // event we returned.
                Range r = new Range(start, true, end, false);
                
                if (log.isTraceEnabled())
                    log.trace(queryNode + " " + uid.getNode());
                
                // don't really want log statement if uid.getNode is null
                if (log.isTraceEnabled() && null != uid.getNode()) {
                    
                    // query node can be null in this case
                    log.trace("Building " + r + " from " + (null == queryNode ? "NoQueryNode" : JexlStringBuildingVisitor.buildQuery(queryNode)) + " actually "
                                    + JexlStringBuildingVisitor.buildQuery(uid.getNode()));
                }
                
                ranges.add(new QueryPlan(uid.getNode(), r));
            }
            return ranges.iterator();
        }
        // else if this is a shard, then range from <shard> to <shard>\x00
        else if (tuple.first().indexOf('_') >= 0) {
            JexlNode myNode = queryNode;
            if (ii.getNode() != null) {
                myNode = ii.getNode();
            }
            if (log.isTraceEnabled() && null != myNode)
                log.trace("Building shard " + new Range(tuple.first(), true, tuple.first() + NULL_BYTE_STRING, false) + " From "
                                + (null == myNode ? "NoQueryNode" : JexlStringBuildingVisitor.buildQuery(myNode)));
            
            return Collections.singleton(new QueryPlan(myNode, new Range(tuple.first(), true, tuple.first() + NULL_BYTE_STRING, false))).iterator();
        }
        // else assume this a day range, then range from <day> to <day>\xff...
        else {
            
            JexlNode myNode = queryNode;
            if (ii.getNode() != null) {
                myNode = ii.getNode();
            }
            
            Range myRange = new Range(tuple.first(), true, tuple.first() + MAX_UNICODE_STRING, false);
            if (log.isTraceEnabled())
                log.trace("Building day" + myRange + " from " + (null == myNode ? "NoQueryNode" : JexlStringBuildingVisitor.buildQuery(myNode)));
            return Collections.singleton(new QueryPlan(myNode, myRange)).iterator();
        }
    }
    
}
