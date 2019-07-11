package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.jexl2.parser.JexlNodes.replaceChild;
import static org.apache.commons.jexl2.parser.JexlNodes.swap;

/**
 * Visitor that enforces node uniqueness within AND or OR expressions. Nodes can be single nodes or subtrees.
 * 
 * <pre>
 * For example:
 * (A || A) => (A)
 * (A && A) => (A)
 * </pre>
 * 
 * This visitor returns a copy of the original query tree, and flattens the copy via the {@link TreeFlatteningRebuildingVisitor}
 * <p>
 * Node traversal is post order.
 */
public class UniqueExpressionTermsVisitor extends RebuildingVisitor {
    
    private int iterations = 0;
    private int duplicates = 0;
    
    private static final Logger log = Logger.getLogger(UniqueExpressionTermsVisitor.class);
    
    /**
     * Entry point without query executability check.
     *
     * @param node
     * @param maxIterations
     * @param <T>
     * @return
     */
    public static <T extends JexlNode> T enforce(T node, int maxIterations) {
        return enforce(node, maxIterations, null, null, null, null, null, null);
    }
    
    /**
     * Entry point with query executability check.
     *
     * Apply this visitor until the query tree stops changing or the maximum number of iterations is exceeded.
     *
     * @param node
     *            - the root node for a query tree
     * @param <T>
     * @return
     */
    public static <T extends JexlNode> T enforce(T node, int maxIterations, ShardQueryConfiguration config, Set<String> indexedFields,
                    Set<String> indexOnlyFields, Set<String> nonEventFields, List<String> debugOutput, MetadataHelper metadataHelper) {
        if (node == null)
            return null;
        
        boolean checkForExecutability = (config != null && indexedFields != null && indexOnlyFields != null && nonEventFields != null && metadataHelper != null);
        boolean isExecutable;
        if (checkForExecutability) {
            isExecutable = ExecutableDeterminationVisitor.isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, debugOutput,
                            metadataHelper);
            
            if (!isExecutable) {
                if (log.isDebugEnabled()) {
                    log.debug("Tried to a visit a query that is not executable. Returning original query tree.");
                }
                return node;
            }
        } else {
            // Assume query is executable, let a later visitor make the actual determination.
            isExecutable = true;
        }
        
        // Operate on copy of query tree
        T copy = (T) copy(node);
        
        UniqueExpressionTermsVisitor visitor = new UniqueExpressionTermsVisitor();
        boolean changesMade = true;
        
        while (visitor.iterations < maxIterations && changesMade && isExecutable) {
            visitor.iterations++;
            
            copy = TreeFlatteningRebuildingVisitor.flatten(copy);
            copy.jjtAccept(visitor, null);
            
            // Changes were made if the two trees are not equals
            changesMade = !TreeEqualityVisitor.isEqual((ASTJexlScript) node, (ASTJexlScript) copy, new TreeEqualityVisitor.Reason());
            
            // Check to see if query is still executable.
            if (checkForExecutability) {
                isExecutable = ExecutableDeterminationVisitor.isExecutable(copy, config, indexedFields, indexOnlyFields, nonEventFields, debugOutput,
                                metadataHelper);
                
                if (!isExecutable) {
                    if (log.isDebugEnabled()) {
                        log.debug("UniqueExpressionTermsVisitor reduced query to a query that will not execute. Returning last query known to execute.");
                    }
                    return node;
                }
            }
            
            node = copy;
        }
        
        // Log if we stopped visiting due to exceeding the max iterations
        if (visitor.iterations >= maxIterations && visitor.iterations > 0) {
            if (log.isDebugEnabled()) {
                log.debug("UniqueExpressionTermsVisitor exceeded maximum iteration (" + maxIterations + ").");
            }
        }
        
        if (log.isDebugEnabled()) {
            log.debug("UniqueExpressionTermsVisitor took " + visitor.iterations + " iterations to fully process query tree.");
            log.debug("UniqueExpressionTermsVisitor removed " + visitor.duplicates + " duplicate terms");
        }
        return node;
    }
    
    @Override
    public Object visit(ASTOrNode orNode, Object data) {
        
        // Post-order traversal
        orNode.childrenAccept(this, data);
        
        List<JexlNode> children = removeDuplicateChildNodes(orNode);
        
        if (children.size() == 1) {
            // If only one child remains, swap child node for current node.
            JexlNode child = children.get(0);
            replaceChild(orNode.jjtGetParent(), orNode, child);
            return child;
        } else {
            // If two or more children remain, replace current children with new children
            ASTOrNode copy = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            copy.image = orNode.image;
            copy.jjtSetParent(orNode.jjtGetParent());
            for (int ii = 0; ii < children.size(); ii++) {
                copy.jjtAddChild(children.get(ii), ii);
            }
            replaceChild(orNode.jjtGetParent(), orNode, copy);
            orNode = copy;
        }
        return orNode;
    }
    
    @Override
    public Object visit(ASTAndNode andNode, Object data) {
        
        // Post-order traversal
        andNode.childrenAccept(this, data);
        
        List<JexlNode> children = removeDuplicateChildNodes(andNode);
        
        if (children.size() == 1) {
            // If only one child remains, swap child node for current node.
            JexlNode child = children.get(0);
            swap(andNode.jjtGetParent(), andNode, child);
            return child;
        } else {
            // If two or more children remain, replace current children with new children
            ASTAndNode copy = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
            copy.image = andNode.image;
            copy.jjtSetParent(andNode.jjtGetParent());
            for (int ii = 0; ii < children.size(); ii++) {
                copy.jjtAddChild(children.get(ii), ii);
            }
            replaceChild(andNode.jjtGetParent(), andNode, copy);
            andNode = copy;
        }
        return andNode;
    }
    
    /**
     * Enforce uniqueness among child nodes.
     *
     * @param node
     *            - an ASTOrNode or ASTAndNode
     */
    private List<JexlNode> removeDuplicateChildNodes(JexlNode node) {
        
        Set<String> childKeys = new HashSet<>();
        List<JexlNode> children = new ArrayList<>();
        for (int ii = 0; ii < node.jjtGetNumChildren(); ii++) {
            JexlNode child = node.jjtGetChild(ii);
            String childKey = TreeHashVisitor.getNodeHash(child).toString();
            if (childKeys.add(childKey)) {
                children.add(child);
            } else {
                this.duplicates++;
            }
        }
        return children;
    }
}
