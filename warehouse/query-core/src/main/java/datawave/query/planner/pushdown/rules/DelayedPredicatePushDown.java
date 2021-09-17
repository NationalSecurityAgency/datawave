package datawave.query.planner.pushdown.rules;

import com.google.common.base.Preconditions;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.planner.pushdown.Cost;
import datawave.query.planner.pushdown.CostEstimator;
import datawave.query.planner.pushdown.PushDownVisitor;
import datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Purpose: Rule that pulls up top level delayed predicates based upon cost
 * 
 * Assumption: We will not do further evaluation on cost, beyond what occurs, below. Therefore, if any complex analysis is needed for cost, it should be
 * performed elsewhere.
 */
public class DelayedPredicatePushDown extends PushDownRule {
    
    protected CostEstimator costEstimator;
    
    private static final Logger log = Logger.getLogger(DelayedPredicatePushDown.class);
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        
        setPushDown((PushDownVisitor) data);
        
        costEstimator = new CostEstimator(parentVisitor);
        
        if (log.isTraceEnabled())
            log.trace("Setting cost estimator");
        
        // don't rewrite yet
        
        ASTJexlScript newScript = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        // for this to work we should only have a single child
        Preconditions.checkArgument(node.jjtGetNumChildren() == 1);
        
        JexlNode child = node.jjtGetChild(0);
        
        if (QueryPropertyMarker.findInstance(node).isType(ASTDelayedPredicate.class)) {
            child = child.jjtGetChild(0);
            child = (JexlNode) child.jjtAccept(this, data);
            
            if (child instanceof ASTReferenceExpression) {
                child = JexlNodes.makeRef(child);
            }
            
            child.jjtSetParent(newScript);
            newScript.jjtAddChild(child, 0);
            return newScript;
        } else
            return node;
        
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // we are a top level And
        SortedSet<Tuple2<JexlNode,Cost>> costEstimates = new TreeSet<>(new CostCompartor());
        Preconditions.checkNotNull(costEstimator);
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            
            costEstimates.add(new Tuple2<>(child, costEstimator.computeCostForSubtree(child)));
            
        }
        
        JexlNode newAnd = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        
        newAnd.jjtSetParent(node.jjtGetParent());
        Iterator<Tuple2<JexlNode,Cost>> tupleIter = costEstimates.iterator();
        
        if (!tupleIter.hasNext())
            return node;
        JexlNode child = tupleIter.next().first();
        
        child.jjtSetParent(newAnd);
        newAnd.jjtAddChild(child, 0);
        int i = 1;
        
        while (tupleIter.hasNext()) {
            
            child = ASTDelayedPredicate.create(tupleIter.next().first());
            
            newAnd.jjtAddChild(child, i);
            child.jjtSetParent(newAnd);
            i++;
        }
        
        return newAnd;
    }
    
    protected class CostCompartor implements Comparator<Tuple2<JexlNode,Cost>> {
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        @Override
        public int compare(Tuple2<JexlNode,Cost> arg0, Tuple2<JexlNode,Cost> arg1) {
            int compareTo = arg0.second().compareTo(arg1.second());
            
            if (compareTo == 0) {
                Integer i = Integer.valueOf(arg0.first().hashCode());
                Integer b = Integer.valueOf(arg1.first().hashCode());
                compareTo = i.compareTo(b);
                
            }
            
            return compareTo;
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.query.planner.pushdown.PushDownRule#getCost(org.apache.commons.jexl2.parser.JexlNode)
     */
    @Override
    public Cost getCost(JexlNode node) {
        return Cost.UNEVALUATED;
    }
    
}
