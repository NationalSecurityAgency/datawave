package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor to expand a query when it is not executable due to an Or that is NON_EXECUTABLE or ERROR state. If an AndNode exists which is a parent, that AND can
 * be distributed to the ORNode to allow the query to be EXECUTABLE. The nodes can be expanded with boolean logic and the distributive property to rewrite A
 * &amp;&amp; (B || C) to (A &amp;&amp; B) || (A &amp;&amp; C).
 */
public class ExecutableExpansionVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(ExecutableExpansionVisitor.class);
    
    private final ShardQueryConfiguration config;
    private final MetadataHelper helper;
    
    public ExecutableExpansionVisitor(ShardQueryConfiguration config, MetadataHelper helper) {
        this.config = config;
        this.helper = helper;
    }
    
    public static ASTJexlScript expand(ASTJexlScript script, ShardQueryConfiguration config, MetadataHelper helper) {
        ExecutableExpansionVisitor visitor = new ExecutableExpansionVisitor(config, helper);
        return (ASTJexlScript) script.jjtAccept(visitor, null);
    }
    
    /**
     * Make a copy of the current queryTree, flattening it and attempting to make it executable if possible by expansion
     *
     * @param node
     *            the top level queryTree node
     * @param data
     *            unused
     * @return a rewritten queryTree with expansion or the original queryTree if expansion was unable to make the queryTree executable
     */
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        // no reason to go any further if the query is executable because that means there couldn't be a problem
        boolean executable = ExecutableDeterminationVisitor.isExecutable(node, config, helper);
        if (!executable) {
            // flatten the tree first
            JexlNode copy = TreeFlatteningRebuildingVisitor.flatten(node);
            // see if we can find a place where expanding the query will make it executable
            super.visit(copy, new ExpansionTracker(null));
            
            // if the query is now executable, return it otherwise ignore the work done
            if (ExecutableDeterminationVisitor.isExecutable(copy, config, helper)) {
                return copy;
            }
        }
        
        return node;
    }
    
    /**
     * Track the current andNode and continue recursively down the tree
     *
     * @param node
     * @param data
     * @return
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        ExpansionTracker tracker = new ExpansionTracker(node);
        
        // because the andNode may be replaced we don't want to call super.visit to handle children processing, but should do so manually and with inspection
        if (node.jjtGetNumChildren() > 0) {
            int childCount = 0;
            // as long as there are more children, and this node is still valid (has a parent) keep visiting children as long as we haven't already failed
            while (childCount < node.jjtGetNumChildren() && node.jjtGetParent() != null && !tracker.isFailedExpansion()) {
                JexlNode child = node.jjtGetChild(childCount);
                if (child instanceof ASTOrNode) {
                    visit((ASTOrNode) child, tracker);
                } else {
                    visit(child, tracker);
                }
                childCount++;
            }
        }
        
        return tracker;
    }
    
    /**
     * if a leaf-most orNode is non-executable evaluate it for expansion and fix the node if possible
     *
     * @param node
     * @param data
     * @return
     */
    @Override
    public Object visit(ASTOrNode node, Object data) {
        // go all the way down first
        super.visit(node, data);
        
        // this should only trigger if the visitor has only run on a subset of the script
        if (!(data instanceof ExpansionTracker)) {
            // the ASTAndNode may exist in the tree but this visitor has not traversed the entire tree
            data = new ExpansionTracker(null);
            ExpansionTracker tracker = (ExpansionTracker) data;
            tracker.setLastAnd(findLastAndNode(tracker, node));
        }
        ExpansionTracker tracker = (ExpansionTracker) data;
        
        // there is no reason to test anything if the query is executable or if a previous Or has failed to be expanded
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(node, config, helper);
        if (canExpand(node, state, tracker)) {
            // this is the first occurrence of an or that is non-executable, attempt to fix it
            tracker.setFailedExpansion(!fix(node, tracker));
        } else {
            // update tracker that a failed expansion happened
            tracker.setFailedExpansion(true);
        }
        
        return data;
    }
    
    /**
     * Test if the current node should be expanded by this visitor. Update the tracker with the current andChild if expansion is possible
     *
     * @param node
     *            the node to test
     * @param tracker
     *            the tracker holding supplementary information about the expansion
     * @return true if the expansion should occur, false otherwise
     */
    private boolean canExpand(JexlNode node, ExecutableDeterminationVisitor.STATE state, ExpansionTracker tracker) {
        // only process if state is ERROR or PARTIAL
        if (!(state == ExecutableDeterminationVisitor.STATE.ERROR || state == ExecutableDeterminationVisitor.STATE.PARTIAL)) {
            return false;
        }
        
        // if deeper down in the tree there was a failed expansion attempt, don't try again
        if (tracker.isFailedExpansion()) {
            return false;
        }
        
        // there must be an andNode further up the tree to distribute into the or
        ASTAndNode lastAnd = tracker.getLastAnd();
        
        // as long as there is a lastAnd there is work to do
        if (lastAnd == null) {
            return false;
        }
        
        // verify there is nothing but compatible nodes between the andNode and the orNode
        JexlNode current = node.jjtGetParent();
        JexlNode last = node;
        while (current != lastAnd) {
            if (!(current instanceof ASTReference || current instanceof ASTReferenceExpression)) {
                return false;
            }
            
            last = current;
            current = current.jjtGetParent();
        }
        
        // if we got here the expansion is good and we should track the andChild for later use
        tracker.setAndChild(last);
        
        return true;
    }
    
    /**
     * When the andNode has not previously been located, call this method to recurse up the queryTree to find it if it exists, updating the tracker with the
     * current child for the position in the tree
     *
     * @param tracker
     *            the tracker for the current node
     * @param node
     *            the node to start from
     * @return the parent andNode if it exists
     */
    private ASTAndNode findLastAndNode(ExpansionTracker tracker, JexlNode node) {
        JexlNode parent = node.jjtGetParent();
        JexlNode last = null;
        while (!(parent == null || (parent instanceof ASTAndNode))) {
            last = parent;
            parent = parent.jjtGetParent();
        }
        
        if (parent != null) {
            tracker.setAndChild(last);
            return (ASTAndNode) parent;
        }
        
        return null;
    }
    
    /**
     * Fix a non-executable orNode if there is a parent AndNode to distribute into the OR. If there is no parent AndNode this cannot be fixed
     *
     * PreCondition: canExpand() has been called on node and has returned true
     *
     * @param node
     *            the orNode to expand
     * @param tracker
     *            the expansion tracker for this node
     * @return true if the orNode has been distributed by an andNode successfully, false otherwise
     */
    private boolean fix(ASTOrNode node, ExpansionTracker tracker) {
        ASTAndNode lastAnd = tracker.getLastAnd();
        
        // distribute the parent terms that are not lastParent into lastParent
        ASTOrNode newOr = distribute(lastAnd, tracker.getAndChild(), node);
        
        if (newOr == null) {
            return false;
        } else {
            // swap the newOr with the lastAnd
            JexlNodes.swap(lastAnd.jjtGetParent(), lastAnd, newOr);
            
            // test that the new expansion has no other necessary expansions nested within it that still need to be evaluated
            if (!ExecutableDeterminationVisitor.isExecutable(newOr, config, helper)) {
                super.visit(newOr, tracker);
            }
            
            return !tracker.isFailedExpansion();
        }
    }
    
    /**
     * Distribute a parent andNode into the orNode, using the andNodeChild to determine which children of the andNode should be distributed and which contain
     * the orNode. orNode should have been passed to canExpand() and returned true prior to calling.
     *
     * @param andNode
     *            the first andNode parent from the orNode
     * @param andNodeChild
     *            the direct child of the andNode that is also a parent of orNode
     * @param orNode
     *            the orNode to distribute the andNode into
     * @return a new orNode to be substituted for the andNode in the original query tree with a logically equivalent (and executable) state
     * @throws IllegalStateException
     *             if a single node path can't be traced between the andNode and orNode
     */
    private ASTOrNode distribute(ASTAndNode andNode, JexlNode andNodeChild, ASTOrNode orNode) {
        // hold the list of nodes that are to be distributed into the orNode grab everything in the and that isn't the branch to be expanded into
        List<JexlNode> otherNodes = new ArrayList<>(andNode.jjtGetNumChildren() - 1);
        for (int i = 0; i < andNode.jjtGetNumChildren(); i++) {
            JexlNode candidate = andNode.jjtGetChild(i);
            if (candidate != andNodeChild) {
                otherNodes.add(candidate);
            }
        }
        
        // create a top level orNode to join together the distributed elements
        ASTOrNode newOr = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        
        // since only ASTReference and ASTReferenceExpression nodes are allowed between the andNode and orNode all the children which should always have a
        // single child check if there is a set of nodes between the child and orNode that need to be passed along
        if (andNodeChild == orNode) {
            // clear the child, its the same as the orNode and there is nothing to bridge
            andNodeChild = null;
        } else if (orNode.jjtGetParent().jjtGetNumChildren() == 1) {
            // replace the orNode with a null so it won't be copied below
            JexlNodes.children(orNode.jjtGetParent(), new JexlNode[0]);
        } else {
            // log an error and abort the distribute, this should never happen
            log.warn("Unexpected number of children on the orNode parent, expected 1 got " + orNode.jjtGetParent().jjtGetNumChildren() + " aborting distribute");
            throw new IllegalStateException("Unexpected number of children on the orNode parent, expected 1 got " + orNode.jjtGetParent().jjtGetNumChildren()
                            + " aborting distribute");
        }
        
        // fetch the minimal expansion terms instead of expanding everything
        List<JexlNode> orTerms = getOrTerms(orNode, andNodeChild);
        
        // everything goes together, abort
        if (orTerms.size() == 1) {
            return null;
        }
        
        // expand each expansion term
        for (int i = 0; i < orTerms.size(); i++) {
            // hold the new combined terms
            ASTAndNode newAnd = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
            
            // get new term
            JexlNode term = orTerms.get(i);
            
            // link the new andNode and the old orNode
            newAnd.jjtAddChild(term, 0);
            term.jjtSetParent(newAnd);
            
            // make a copy of each otherNode and add them to the newAnd
            int otherNodeCount = 0;
            for (JexlNode otherNode : otherNodes) {
                JexlNode copy = RebuildingVisitor.copy(otherNode);
                copy.jjtSetParent(newAnd);
                newAnd.jjtAddChild(copy, otherNodeCount + 1);
                otherNodeCount++;
            }
            
            // attach newAnd to the newOr
            newAnd.jjtSetParent(newOr);
            newOr.jjtAddChild(newAnd, newOr.jjtGetNumChildren());
        }
        
        return newOr;
    }
    
    /**
     * Get the list of breakout terms from an orNode where each term is Executable or is the grouping of non-executable elements in a new orNode, each bridged
     * appropriately
     *
     * @param orNode
     *            the orNode to get expansion terms from
     * @param bridgeFrom
     *            the JexlNode to begin all bridges or null if there should be no bridging
     * @return a non-null list of bridged JexlNode that should be part of the expansion
     */
    private List<JexlNode> getOrTerms(ASTOrNode orNode, JexlNode bridgeFrom) {
        List<JexlNode> terms = new ArrayList<>();
        List<JexlNode> nonExecutablePool = new ArrayList<>();
        for (int i = 0; i < orNode.jjtGetNumChildren(); i++) {
            JexlNode child = orNode.jjtGetChild(i);
            if (ExecutableDeterminationVisitor.isExecutable(child, config, helper)) {
                // break out
                terms.add(bridge(child, bridgeFrom));
            } else {
                // add to pool of non-broken out
                nonExecutablePool.add(child);
            }
        }
        
        JexlNode poolNode = null;
        // add the nonExecutablePool elements to a newOr that will be carried forward if there is more than one element
        if (nonExecutablePool.size() > 1) {
            poolNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            int childCount = 0;
            for (JexlNode nonExecutableChild : nonExecutablePool) {
                nonExecutableChild.jjtSetParent(poolNode);
                poolNode.jjtAddChild(nonExecutableChild, childCount++);
            }
        } else {
            poolNode = nonExecutablePool.get(0);
        }
        
        terms.add(bridge(poolNode, bridgeFrom));
        
        return terms;
    }
    
    /**
     * bridge the gap between an orNode child and the expansion andNode so as to maintain tree continuity
     *
     * @param bridgeTo
     *            the node to attach to the leaf of the bridge
     * @param bridgeFrom
     *            the node to begin bridging from, may be null to prevent bridging
     * @return a JexlNode tree fragment representing a bridge of JexlNodes from bridgeFrom to bridgeTo, or bridgeTo if bridgeFrom is null
     */
    private JexlNode bridge(JexlNode bridgeTo, JexlNode bridgeFrom) {
        JexlNode bridge = bridgeTo;
        if (bridgeFrom != null) {
            // make a copy of the extended region of the tree and add the orChild in bypassing the orNode
            bridge = RebuildingVisitor.copy(bridgeFrom);
            
            // loop down through the copied extension until we get to the leaf
            JexlNode current = bridge.jjtGetChild(0);
            while (current != null && current.jjtGetNumChildren() != 0) {
                current = current.jjtGetChild(0);
            }
            
            // sanity check
            if (current == null) {
                log.error("Unexpected bridge format between andNode and orNode");
                return null;
            }
            
            // attach the oldOr to the end of the extension
            bridgeTo.jjtSetParent(current);
            current.jjtAddChild(bridgeTo, 0);
        }
        
        return bridge;
    }
    
    /**
     * Track the state of the current expansion efforts
     */
    protected static class ExpansionTracker {
        /**
         * Last andNode encountered while recursing the tree
         */
        private ASTAndNode lastAnd;
        
        /**
         * If an expansion attempt was made and failed, set this to true
         */
        private boolean failedExpansion = false;
        
        /**
         * The direct child of an andNode that is a parent of the current orNode being evaluated
         */
        private JexlNode andChild = null;
        
        public ExpansionTracker(ASTAndNode lastAnd) {
            this.lastAnd = lastAnd;
        }
        
        public ASTAndNode getLastAnd() {
            return lastAnd;
        }
        
        public void setLastAnd(ASTAndNode lastAnd) {
            this.lastAnd = lastAnd;
        }
        
        public boolean isFailedExpansion() {
            return failedExpansion;
        }
        
        public void setFailedExpansion(boolean failedExpansion) {
            this.failedExpansion = failedExpansion;
        }
        
        public JexlNode getAndChild() {
            return andChild;
        }
        
        public void setAndChild(JexlNode andChild) {
            this.andChild = andChild;
        }
    }
}
