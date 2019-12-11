package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Visitor builds a list of all delayed sub trees within a script, and removes any nodes which are not based on index only lookups. This visitor will
 * modify/destroy the tree so a copy should always be used
 */
public class DelayedNonEventSubTreeVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(DelayedNonEventSubTreeVisitor.class);
    
    private Set<String> nonEventFields;
    private Set<JexlNode> delayedSubTrees;
    private Set<String> foundNonEventFields;
    
    public static DelayedNonEventSubTreeVisitor processDelayedSubTrees(ASTJexlScript script, Set<String> nonEventFields) {
        // create a safe copy to rip apart
        ASTJexlScript copy = (ASTJexlScript) RebuildingVisitor.copy(script);
        
        // run the visitor on the copy
        DelayedNonEventSubTreeVisitor visitor = new DelayedNonEventSubTreeVisitor(nonEventFields);
        copy.jjtAccept(visitor, null);
        
        return visitor;
    }
    
    private DelayedNonEventSubTreeVisitor(Set<String> nonEventFields) {
        this.nonEventFields = nonEventFields;
        delayedSubTrees = new HashSet<>();
        foundNonEventFields = new HashSet<>();
    }
    
    public Set<JexlNode> getDelayedSubTrees() {
        return delayedSubTrees;
    }
    
    public Set<String> getFoundNonEventFields() {
        return foundNonEventFields;
    }
    
    /**
     * Check if the node is an instance of a DelayedPredicate and if so capture the subtree and the found index only fields
     * 
     * @param node
     * @param data
     * @return
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (JexlASTHelper.isDelayedPredicate(node)) {
            // check for expected form
            if (node.jjtGetNumChildren() == 2) {
                // grab the sub tree off the delayed node for processing, the second node is always the one that
                JexlNode candidate = node.jjtGetChild(1);
                
                // get all the index only fields nested underneath and add them to the tracked list of nodes
                boolean keep = false;
                for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(candidate)) {
                    if (nonEventFields.contains(identifier.image)) {
                        foundNonEventFields.add(identifier.image);
                        keep = true;
                    }
                }
                
                // only keep sub trees that have nonEventFields
                if (keep) {
                    delayedSubTrees.add(candidate);
                }
            } else {
                // should never get here, but log a warning to validate the assumption
                log.warn("Unexpected structure for DelayedPredicate:\n" + JexlStringBuildingVisitor.buildQuery(node));
            }
        }
        
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return prune(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return prune(node, data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return prune(node, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return prune(node, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return prune(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return prune(node, data);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return prune(node, data);
    }
    
    /**
     * Prune any nodes that are not against the index only fields or are against functions
     * 
     * @param node
     * @param data
     * @return
     */
    private Object prune(JexlNode node, Object data) {
        JexlNode parent = node.jjtGetParent();
        if (node instanceof ASTFunctionNode && parent != null) {
            JexlNodes.removeFromParent(parent, node);
            return data;
        } else {
            try {
                String identifier = JexlASTHelper.getIdentifier(node);
                if (identifier != null && !nonEventFields.contains(identifier)) {
                    // remove this node from the parent
                    if (parent != null) {
                        JexlNodes.removeFromParent(parent, node);
                        
                        // reprocess the parent since the lengths have shifted, so can't continue from this point
                        parent.jjtAccept(this, node);
                    }
                    
                    // if parent is null this is the top level node and there is nothing to do since the top level node is not delayed so this can't back
                    // propagate for evaluation anyway
                }
            } catch (NoSuchElementException e) {
                // no-op, caught when there is no identifier, which is fine
            }
        }
        
        // not pruned, keep going down the tree
        return super.visit(node, data);
    }
}
