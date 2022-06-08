package datawave.query.jexl.visitors.whindex;

import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.RebuildingVisitor;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a visitor which is used to fully distribute anded nodes into a given node. The visitor will only distribute the anded nodes to those descendant nodes
 * within the tree with which they are not already anded (via a whindex).
 */
class DistributeAndedNodesVisitor extends RebuildingVisitor {
    private JexlNode initialNode = null;
    private final List<JexlNode> andedNodes;
    private final Map<JexlNode,WhindexTerm> whindexNodes;
    private final Multimap<String,String> fieldValueMappings;
    
    private static class DistAndData {
        boolean finalized = false;
        Set<JexlNode> usedAndedNodes = new HashSet<>();
    }
    
    private DistributeAndedNodesVisitor(List<JexlNode> andedNodes, Map<JexlNode,WhindexTerm> whindexNodes, Multimap<String,String> fieldValueMappings) {
        this.andedNodes = new ArrayList<>(andedNodes);
        this.whindexNodes = whindexNodes;
        this.fieldValueMappings = fieldValueMappings;
        
        addEvaluationOnlyFieldValueNodes();
    }
    
    private static class PlaceholderEvaluationOnly extends ASTEvaluationOnly {
        private final JexlNode origNode;
        
        public PlaceholderEvaluationOnly(JexlNode origNode) {
            this.origNode = origNode;
        }
        
        public JexlNode getOrigNode() {
            return origNode;
        }
        
        public JexlNode getEvaluationOnlyNode() {
            return ASTEvaluationOnly.create(RebuildingVisitor.copy(origNode));
        }
    }
    
    // this adds EvaluationOnly placeholders for those nodes which represent
    // one to many field mappings. This is to ensure that evaluation only
    // terms are added when an incomplete whindex is generated
    private void addEvaluationOnlyFieldValueNodes() {
        Set<JexlNode> fieldValueNodes = new HashSet<>();
        for (WhindexTerm whindexTerm : whindexNodes.values()) {
            if (fieldValueMappings.get(whindexTerm.getNewFieldName()).size() > 1) {
                fieldValueNodes.add(JexlASTHelper.dereference(whindexTerm.getFieldValueNode()));
            }
        }
        
        Set<JexlNode> matchingAndedNodes = new HashSet<>(andedNodes);
        matchingAndedNodes.retainAll(fieldValueNodes);
        
        for (JexlNode andedNode : matchingAndedNodes) {
            andedNodes.add(new PlaceholderEvaluationOnly(andedNode));
        }
    }
    
    /**
     * Distribute the anded node, making sure to 'and' it in at the highest possible level of the tree. This version takes a map of whindex nodes to their
     * component nodes, so that we can better check whindex nodes to see if they already include the anded node. That is to say, we will not 'and' a whindex
     * node with one of it's component nodes.
     *
     * @param script
     *            The node that we will be distributing the anded nodes into
     * @param andedNodes
     *            The nodes which we will be distributing into the root node
     * @param whindexNodes
     *            A map of generated whindex jexl nodes to the whindex term used to create that node
     * @param fieldValueMappings
     *            A multimap of the new fields, to the values that they represent
     * @return An updated script with the anded nodes distributed throughout
     */
    public static JexlNode distributeAndedNode(JexlNode script, List<JexlNode> andedNodes, Map<JexlNode,WhindexTerm> whindexNodes,
                    Multimap<String,String> fieldValueMappings) {
        DistributeAndedNodesVisitor visitor = new DistributeAndedNodesVisitor(andedNodes, whindexNodes, fieldValueMappings);
        DistributeAndedNodesVisitor.DistAndData foundData = new DistributeAndedNodesVisitor.DistAndData();
        JexlNode resultNode = (JexlNode) script.jjtAccept(visitor, foundData);
        
        if (!foundData.finalized) {
            // if we have both the placeholder node, and the original node in our list of usedAndedNodes, remove the placeholder node so that it gets anded in
            dedupePlaceholderNodes(foundData.usedAndedNodes);
            
            if (!foundData.usedAndedNodes.containsAll(visitor.andedNodes)) {
                List<JexlNode> nodes = visitor.andedNodes.stream().filter(node -> !foundData.usedAndedNodes.contains(node))
                                .map(DistributeAndedNodesVisitor::resolvePlaceholderAndRebuild).collect(Collectors.toList());
                nodes.add(resultNode);
                
                return WhindexVisitor.createUnwrappedAndNode(nodes);
            }
        }
        
        return resultNode;
    }
    
    /**
     * Checks each of the child nodes, and determines how the anded nodes should be applied.
     *
     * @param node
     *            The node that we will be distributing the anded nodes into
     * @param data
     *            The nodes which we will be distributing into the root node
     * @return An updated script with the anded nodes distributed throughout
     */
    @Override
    public Object visit(ASTOrNode node, Object data) {
        DistributeAndedNodesVisitor.DistAndData parentData = (DistributeAndedNodesVisitor.DistAndData) data;
        
        if (initialNode == null || initialNode instanceof ASTReference || initialNode instanceof ASTReferenceExpression) {
            initialNode = node;
        }
        
        // if this node is one of the anded nodes, or a whindex
        // comprised of one of the anded nodes, halt recursion
        List<JexlNode> usedAndedNodes = usesAndedNodes(node);
        usedAndedNodes.removeIf(x -> x instanceof PlaceholderEvaluationOnly);
        if (!usedAndedNodes.isEmpty()) {
            parentData.usedAndedNodes.addAll(usedAndedNodes);
            return node;
        }
        
        // don't descend into whindex nodes, and don't copy them
        // this logic is dependent upon identifying whindex nodes by their address
        if (whindexNodes.containsKey(node)) {
            return node;
        }
        
        boolean rebuildNode = false;
        
        // check each child node
        List<JexlNode> nodesMissingEverything = new ArrayList<>();
        List<JexlNode> nodesWithEverything = new ArrayList<>();
        Map<JexlNode,List<JexlNode>> nodesMissingSomething = new LinkedHashMap<>();
        for (JexlNode child : JexlNodes.children(node)) {
            DistributeAndedNodesVisitor.DistAndData foundData = new DistributeAndedNodesVisitor.DistAndData();
            JexlNode processedChild = (JexlNode) child.jjtAccept(this, foundData);
            
            if (processedChild != child) {
                rebuildNode = true;
            }
            
            if (foundData.usedAndedNodes.isEmpty()) {
                nodesMissingEverything.add(processedChild);
            } else if (!foundData.usedAndedNodes.containsAll(andedNodes)) {
                List<JexlNode> missingAndedNodes = new ArrayList<>(andedNodes);
                missingAndedNodes.removeAll(foundData.usedAndedNodes);
                nodesMissingSomething.put(processedChild, missingAndedNodes);
            } else {
                nodesWithEverything.add(processedChild);
            }
        }
        
        // if none of the children are missing anything, we're done
        if (nodesWithEverything.size() == node.jjtGetNumChildren()) {
            parentData.usedAndedNodes.addAll(andedNodes);
            
            // set this to indicate that all anded nodes have been distributed
            parentData.finalized = true;
            
            if (rebuildNode) {
                return WhindexVisitor.createUnwrappedOrNode(nodesWithEverything);
            } else {
                return node;
            }
        }
        // if all of the children are missing everything, we're done
        // note: we shouldn't need to rebuild the or node because if the children
        // are missing everything, it implies that the children were left as-is
        else if (nodesMissingEverything.size() == node.jjtGetNumChildren()) {
            return node;
        }
        
        // if we got here, then there are some nodes missing SOMETHING, and we have work to do
        List<JexlNode> rebuiltChildren = new ArrayList<>();
        
        // for children missing at least one andedNode -> go through each one, and make a new call to 'distributeAndedNode' passing only the missing
        // andedNodes
        for (Map.Entry<JexlNode,List<JexlNode>> childEntry : nodesMissingSomething.entrySet()) {
            rebuiltChildren.add(DistributeAndedNodesVisitor.distributeAndedNode(childEntry.getKey(), childEntry.getValue(), whindexNodes, fieldValueMappings));
        }
        
        if (!nodesMissingEverything.isEmpty()) {
            // for children missing everything -> 'or' them together, then 'and' them with the full set of andedNodes
            List<JexlNode> nodeList = andedNodes.stream().map(DistributeAndedNodesVisitor::resolvePlaceholderAndRebuild).collect(Collectors.toList());
            nodeList.add(WhindexVisitor.createUnwrappedOrNode(nodesMissingEverything));
            
            rebuiltChildren.add(WhindexVisitor.createUnwrappedAndNode(nodeList));
        }
        
        // for children with everything -> keep those as-is
        rebuiltChildren.addAll(nodesWithEverything);
        
        parentData.usedAndedNodes.addAll(andedNodes);
        
        // set this to indicate that all anded nodes have been distributed
        parentData.finalized = true;
        
        return WhindexVisitor.createUnwrappedOrNode(rebuiltChildren);
    }
    
    /**
     * Checks each of the child nodes, and determines how the anded nodes should be applied.
     *
     * @param node
     *            The node that we will be distributing the anded nodes into
     * @param data
     *            The nodes which we will be distributing into the root node
     * @return An updated script with the anded nodes distributed throughout
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        DistributeAndedNodesVisitor.DistAndData parentData = (DistributeAndedNodesVisitor.DistAndData) data;
        
        if (initialNode == null || initialNode instanceof ASTReference || initialNode instanceof ASTReferenceExpression) {
            initialNode = node;
        }
        
        // if this node is one of the anded nodes, or a whindex
        // comprised of one of the anded nodes, halt recursion
        List<JexlNode> usedAndedNodes = usesAndedNodes(node);
        usedAndedNodes.removeIf(x -> x instanceof PlaceholderEvaluationOnly);
        if (!usedAndedNodes.isEmpty()) {
            parentData.usedAndedNodes.addAll(usedAndedNodes);
            return node;
        }
        
        // don't descend into whindex nodes, and don't copy them
        // this logic is dependent upon identifying whindex nodes by their address
        if (whindexNodes.containsKey(node)) {
            return node;
        }
        
        // check each child node to see how many of the desired andedNodes are present
        List<JexlNode> rebuiltChildren = new ArrayList<>();
        for (JexlNode child : JexlNodes.children(node)) {
            DistributeAndedNodesVisitor.DistAndData foundData = new DistributeAndedNodesVisitor.DistAndData();
            rebuiltChildren.add((JexlNode) child.jjtAccept(this, foundData));
            
            parentData.usedAndedNodes.addAll(foundData.usedAndedNodes);
        }
        
        // if we have both the placeholder node, and the original node in our list of usedAndedNodes, remove the placeholder node so that it gets anded in
        dedupePlaceholderNodes(parentData.usedAndedNodes);
        
        // are some anded nodes missing, and is this the initial node?
        if (!parentData.usedAndedNodes.containsAll(andedNodes) && node.equals(initialNode)) {
            // 'and' with the missing anded nodes, and return
            List<JexlNode> nodes = andedNodes.stream().filter(andedNode -> !parentData.usedAndedNodes.contains(andedNode))
                            .map(DistributeAndedNodesVisitor::resolvePlaceholderAndRebuild).collect(Collectors.toList());
            nodes.add(node);
            
            // this is probably unnecessary, but to be safe, let's set it
            parentData.usedAndedNodes.addAll(andedNodes);
            
            // set this to indicate that all anded nodes have been distributed
            parentData.finalized = true;
            
            return WhindexVisitor.createUnwrappedAndNode(nodes);
        }
        
        return WhindexVisitor.createUnwrappedAndNode(rebuiltChildren);
    }
    
    private static void dedupePlaceholderNodes(Set<JexlNode> usedAndedNodes) {
        Set<JexlNode> originalNodes = usedAndedNodes.stream().filter(x -> !(x instanceof PlaceholderEvaluationOnly)).collect(Collectors.toSet());
        usedAndedNodes.removeIf(x -> x instanceof PlaceholderEvaluationOnly && originalNodes.contains(((PlaceholderEvaluationOnly) x).getOrigNode()));
    }
    
    private static JexlNode resolvePlaceholderAndRebuild(JexlNode node) {
        if (node instanceof PlaceholderEvaluationOnly) {
            node = ((PlaceholderEvaluationOnly) node).getEvaluationOnlyNode();
        } else {
            node = RebuildingVisitor.copy(node);
        }
        return node;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        visitInternal(node, data);
        return node;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        visitInternal(node, data);
        return node;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        visitInternal(node, data);
        return node;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        visitInternal(node, data);
        return node;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        visitInternal(node, data);
        return node;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        visitInternal(node, data);
        return node;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        visitInternal(node, data);
        return node;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        visitInternal(node, data);
        return node;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        visitInternal(node, data);
        return node;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        visitInternal(node, data);
        return super.visit(node, data);
    }
    
    /**
     * Used to determine whether this is a whindex node, and if so, which of the anded nodes does it have as components
     *
     * @param node
     *            The node to check for anded components
     * @return A list of anded jexl nodes used to create the whindex node
     */
    private List<JexlNode> usesAndedNodes(JexlNode node) {
        List<JexlNode> usedAndedNodes = new ArrayList<>();
        
        // if this is a whindex node, an anded node counts as used if the whindex term contains the anded
        if (whindexNodes.containsKey(node)) {
            WhindexTerm whindexTerm = whindexNodes.get(node);
            
            for (JexlNode andedNode : andedNodes) {
                if (whindexTerm.contains(andedNode)) {
                    usedAndedNodes.add(andedNode);
                }
            }
        }
        // if this is a normal node, an anded node counts as used automatically if it's a PlaceholderEvaluationOnly node
        else {
            for (JexlNode andedNode : andedNodes) {
                if (andedNode instanceof PlaceholderEvaluationOnly) {
                    usedAndedNodes.add(andedNode);
                }
            }
        }
        return usedAndedNodes;
    }
    
    private void visitInternal(JexlNode node, Object data) {
        if (initialNode == null || initialNode instanceof ASTReference || initialNode instanceof ASTReferenceExpression) {
            initialNode = node;
        }
        
        DistributeAndedNodesVisitor.DistAndData parentData = (DistributeAndedNodesVisitor.DistAndData) data;
        parentData.usedAndedNodes.addAll(usesAndedNodes(node));
    }
}
