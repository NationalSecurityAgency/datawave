package datawave.query.jexl.visitors;

import datawave.query.Constants;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.planner.QueryPlanMetadata;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.log4j.Logger;

/**
 * Builds {@link datawave.query.planner.QueryPlanMetadata} from an ASTJexlScript. The plan metadata may become stale if the underlying query tree changes.
 */
public class QueryPlanMetadataVisitor extends BaseVisitor {
    
    private final QueryPlanMetadata planMetadata;
    
    private static final Logger log = Logger.getLogger(QueryPlanMetadataVisitor.class);
    
    private QueryPlanMetadataVisitor() {
        planMetadata = new QueryPlanMetadata();
    }
    
    /**
     * Build {@link QueryPlanMetadata} for the provided script.
     *
     * @param script
     *            query tree
     * @return the QueryPlanMetadata for this script
     */
    public static QueryPlanMetadata getQueryPlanMetadata(ASTJexlScript script) {
        QueryPlanMetadataVisitor visitor = new QueryPlanMetadataVisitor();
        script.jjtAccept(visitor, null);
        return visitor.getPlanMetadata();
    }
    
    public QueryPlanMetadata getPlanMetadata() {
        return planMetadata;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        planMetadata.incrementOrNodeCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        planMetadata.incrementAndNodeCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        planMetadata.incrementEqualsNodeCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        planMetadata.incrementNotEqualsNodeCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        planMetadata.incrementLessThanCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        planMetadata.incrementGreaterThanCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        planMetadata.incrementLessThanOrEqualsCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        planMetadata.incrementGreaterThanOrEqualsCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        planMetadata.incrementRegexEqualsCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        planMetadata.incrementRegexNotEqualsCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        planMetadata.incrementNotNodeCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        planMetadata.incrementFunctionCount();
        node.childrenAccept(this, data);
        return data;
    }
    
    // Note: ASTReference and ASTReferenceExpression nodes are not counted.
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        
        if (node.image.equals(ASTDelayedPredicate.class.getSimpleName())) {
            
            planMetadata.incrementDelayedPredicateCount();
            
        } else if (node.image.equals(ASTEvaluationOnly.class.getSimpleName())) {
            
            planMetadata.incrementEvaluationOnlyCount();
            
        } else if (node.image.equals(IndexHoleMarkerJexlNode.class.getSimpleName())) {
            
            planMetadata.incrementIndexHoldCount();
            
        } else if (node.image.equals(ExceededOrThresholdMarkerJexlNode.class.getSimpleName())) {
            
            planMetadata.incrementExceededOrThresholdCount();
            
        } else if (node.image.equals(ExceededTermThresholdMarkerJexlNode.class.getSimpleName())) {
            
            planMetadata.incrementExceededTermThresholdCount();
            
        } else if (node.image.equals(ExceededValueThresholdMarkerJexlNode.class.getSimpleName())) {
            
            planMetadata.incrementExceededValueThresholdCount();
            
        } else if (node.image.equals("includeRegex")) {
            
            planMetadata.incrementIncludeRegexCount();
            
        } else if (node.image.equals("excludeRegex")) {
            
            planMetadata.incrementExcludeRegexCount();
            
        } else if (node.image.equals("isNull")) {
            
            planMetadata.incrementIsNullCount();
            
        } else if (node.image.equals("betweenDates")) {
            
            planMetadata.incrementBetweenDatesCount();
            
        } else if (node.image.equals("betweenLoadDates")) {
            
            planMetadata.incrementBetweenLoadDatesCount();
            
        } else if (node.image.equals("matchesAtLeastCountOf")) {
            
            planMetadata.incrementMatchesAtLeastCount();
            
        } else if (node.image.equals("timeFunction")) {
            
            planMetadata.incrementTimeFunctionCount();
            
        } else if (node.image.equals("includeText")) {
            
            planMetadata.incrementIncludeTextCount();
            
        } else if (node.image.equals(Constants.ANY_FIELD)) {
            
            planMetadata.incrementAnyFieldCount();
            
        } else {
            
            if (log.isTraceEnabled())
                log.trace(getClass().getSimpleName() + " could not parse ASTIdentifier with image: " + node.image);
        }
        return data;
    }
    
}
