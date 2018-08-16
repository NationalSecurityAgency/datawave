package datawave.query.jexl.visitors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import datawave.data.type.NoOpType;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.composite.Composite;
import datawave.query.composite.CompositeRange;
import datawave.query.composite.CompositeUtils;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTCompositePredicate;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.jexl2.parser.JexlNodes.children;

/**
 * This is a visitor which runs across the query tree and creates composite jexl nodes where applicable. Composite field mappings are determined via ingest
 * configuration, and can be used to create a combined index from multiple fields.
 *
 */
public class ExpandCompositeTerms extends RebuildingVisitor {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(ExpandCompositeTerms.class);
    
    private final ShardQueryConfiguration config;
    
    private HashMap<JexlNode,Composite> jexlNodeToCompMap = new HashMap<>();
    
    private static class ExpandData {
        public boolean foundComposite = false;
        public Multimap<String,JexlNode> andedNodes = LinkedHashMultimap.create();
        public Multimap<String,JexlNode> usedAndedNodes = LinkedHashMultimap.create();
    }
    
    private ExpandCompositeTerms(ShardQueryConfiguration config) {
        Preconditions.checkNotNull(config);
        this.config = config;
    }
    
    /**
     * Expand all nodes which have multiple dataTypes for the field.
     *
     * @param config
     *            Configuration parameters relevant to our query
     * @param script
     *            The jexl node representing the query
     * @return An expanded version of the passed-in script containing composite nodes
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T expandTerms(ShardQueryConfiguration config, MetadataHelper helper, T script) {
        
        ExpandCompositeTerms visitor = new ExpandCompositeTerms(config);
        
        // need to flatten the tree so i get all and nodes at the same level
        script = TreeFlatteningRebuildingVisitor.flatten(script);
        
        if (null == visitor.config.getCompositeToFieldMap()) {
            QueryException qe = new QueryException(DatawaveErrorCode.DATATYPESFORINDEXFIELDS_MULTIMAP_MISSING);
            throw new DatawaveFatalQueryException(qe);
        }
        
        return (T) script.jjtAccept(visitor, new ExpandData());
    }
    
    /**
     * Descends into each of the child nodes, and rebuilds the 'or' node with both the unmodified and modified nodes. Ancestor anded nodes are passed down to
     * each child, and the foundComposite flag is passed up from the children.
     *
     * @param node
     *            An 'or' node from the original script
     * @param data
     *            ExpandData, containing ancestor anded nodes, used anded nodes, and a flag indicating whether composites were found
     * @return An expanded version of the 'or' node containing composite nodes, if found, or the original in node, if not found
     */
    @Override
    public Object visit(ASTOrNode node, Object data) {
        ExpandData parentData = (ExpandData) data;
        
        // if we only have one child, just pass through
        // this shouldn't ever really happen, but it could
        if (node.jjtGetNumChildren() == 1)
            return super.visit(node, data);
        
        // iterate over the children and attempt to create composites
        List<JexlNode> unmodifiedNodes = new ArrayList<>();
        List<JexlNode> modifiedNodes = new ArrayList<>();
        for (JexlNode child : children(node)) {
            ExpandData eData = new ExpandData();
            
            // add the anded leaf nodes from our ancestors
            eData.andedNodes.putAll(parentData.andedNodes);
            
            JexlNode processedNode = (JexlNode) child.jjtAccept(this, eData);
            
            // if composites were made, save the processed node,
            // and keep track of the used anded nodes
            if (eData.foundComposite) {
                modifiedNodes.add(processedNode);
                parentData.foundComposite = true;
                parentData.usedAndedNodes.putAll(eData.usedAndedNodes);
            } else
                unmodifiedNodes.add(child);
        }
        
        List<JexlNode> processedNodes = new ArrayList<>();
        processedNodes.addAll(unmodifiedNodes);
        processedNodes.addAll(modifiedNodes);
        
        // if we found a composite, rebuild the or node,
        // otherwise, return the original or node
        if (parentData.foundComposite) {
            return createUnwrappedOrNode(processedNodes);
        } else
            return node;
    }
    
    /**
     * Rebuilds the current 'and' node, and attempts to create the best composites from the leaf and ancestor anded nodes available. First, we descend into the
     * non-leaf nodes, and keep track of which leaf and anded nodes are used. We then attempt to create composites from the remaining leaf and anded nodes.
     * Finally, any leftover, unused leaf nodes are anded at this level, while the used leaf nodes are passed down to the descendants and anded where
     * appropriate.
     *
     * @param node
     *            An 'and' node from the original script
     * @param data
     *            ExpandData, containing ancestor anded nodes, used anded nodes, and a flag indicating whether composites were found
     * @return An expanded version of the 'and' node containing composite nodes, if found, or the original in node, if not found
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        ExpandData parentData = (ExpandData) data;
        
        // ignore marked nodes
        if (QueryPropertyMarkerVisitor.instanceOfAny(node))
            return node;
        
        // if we only have one child, just pass through
        // this shouldn't ever really happen, but it could
        if (node.jjtGetNumChildren() == 1)
            return super.visit(node, data);
        
        // first, find all leaf nodes
        // note: an 'and' node defining a range over a single term is considered a leaf node for our purposes
        List<JexlNode> nonLeafNodes = new ArrayList<>();
        Multimap<String,JexlNode> leafNodes = getLeafNodes(node, nonLeafNodes);
        
        // if this is a 'leaf' range node, check to see if a composite can be made
        if (leafNodes.size() == 1 && leafNodes.containsValue(node)) {
            // attempt to build a composite
            return visitLeafNode(node, parentData);
        }
        // otherwise, process the 'and' node as usual
        else {
            
            Multimap<String,JexlNode> usedLeafNodes = LinkedHashMultimap.create();
            
            // process the non-leaf nodes first
            List<JexlNode> processedNonLeafNodes = processNonLeafNodes(parentData, nonLeafNodes, leafNodes, usedLeafNodes);
            
            // remove the used nodes from the leaf and anded nodes
            leafNodes.values().removeAll(usedLeafNodes.values());
            parentData.andedNodes.values().removeAll(parentData.usedAndedNodes.values());
            
            // next, process the remaining leaf nodes
            List<JexlNode> processedLeafNodes = processUnusedLeafNodes(parentData, leafNodes, usedLeafNodes);
            
            // again, remove the used nodes from the leaf and anded nodes
            leafNodes.values().removeAll(usedLeafNodes.values());
            parentData.andedNodes.values().removeAll(parentData.usedAndedNodes.values());
            
            // rebuild the node if composites are found
            if (parentData.foundComposite) {
                List<JexlNode> processedNodes = new ArrayList<>();
                processedNodes.addAll(processedLeafNodes);
                processedNodes.addAll(processedNonLeafNodes);
                
                // rebuild the node
                JexlNode rebuiltNode = createUnwrappedAndNode(processedNodes);
                
                // distribute the used nodes into the rebuilt node
                if (!usedLeafNodes.values().isEmpty()) {
                    // first we need to trim the used nodes to eliminate any wrapping nodes
                    // i.e. reference, reference expression, or single child and/or nodes
                    List<JexlNode> leafNodesToDistribute = usedLeafNodes.values().stream().map(this::getLeafNode).collect(Collectors.toList());
                    rebuiltNode = DistributeAndedNodes.distributeAndedNode(rebuiltNode, leafNodesToDistribute, jexlNodeToCompMap);
                }
                
                return rebuiltNode;
            }
            
            return node;
        }
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return visitLeafNode(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return visitLeafNode(node, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return visitLeafNode(node, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return visitLeafNode(node, data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return visitLeafNode(node, data);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return visitLeafNode(node, data);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return visitLeafNode(node, data);
    }
    
    // let's avoid != for now
    @Override
    public Object visit(ASTNENode node, Object data) {
        return node;
    }
    
    // let's avoid not nodes for now
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return node;
    }
    
    // don't descend into function nodes
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return node;
    }
    
    // don't descend into delayed predicates
    @Override
    public Object visit(ASTReference node, Object data) {
        // ignore marked nodes
        if (!QueryPropertyMarkerVisitor.instanceOfAny(node)) {
            return super.visit(node, data);
        }
        return node;
    }
    
    // don't descend into delayed predicates
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        // ignore marked nodes
        if (!QueryPropertyMarkerVisitor.instanceOfAny(node))
            return super.visit(node, data);
        return node;
    }
    
    /**
     * Attempts to create composites using both the leaf nodes and the anded nodes from our ancestors. Each of the composites created must contain at least one
     * of the leaf nodes in order to be valid. The used leaf nodes are passed back via the usedLeafNodes param. The used anded nodes are passed back via the
     * parentData.
     *
     * @param parentData
     *            Contains the ancestor anded nodes, anded nodes used to create the returned composites, and a flag indicating whether composites were found
     * @param nonLeafNodes
     *            A collection of non-leaf child nodes, from the parent node
     * @param leafNodes
     *            A multimap of leaf child nodes, keyed by field name, from the parent node
     * @param usedLeafNodes
     *            A multimap of used leaf child nodes, keyed by field name, used to create the returned composites
     * @return A list of modified and unmodified non-leaf child nodes, from the parent node
     */
    private List<JexlNode> processNonLeafNodes(ExpandData parentData, Collection<JexlNode> nonLeafNodes, Multimap<String,JexlNode> leafNodes,
                    Multimap<String,JexlNode> usedLeafNodes) {
        // descend into the child nodes, passing the anded leaf nodes down,
        // in order to determine whether or not composites can be made
        List<JexlNode> unmodifiedNodes = new ArrayList<>();
        List<JexlNode> modifiedNodes = new ArrayList<>();
        for (JexlNode nonLeafNode : nonLeafNodes) {
            ExpandData eData = new ExpandData();
            
            // add the anded leaf nodes from our ancestors
            eData.andedNodes.putAll(parentData.andedNodes);
            
            // add our anded leaf nodes
            eData.andedNodes.putAll(leafNodes);
            
            // descend into the non-leaf node to see if composites can be made
            JexlNode processedNode = (JexlNode) nonLeafNode.jjtAccept(this, eData);
            
            // if composites were made, save the processed node, and determine which leaf
            // nodes were used from this and node (if any)
            if (eData.foundComposite) {
                parentData.foundComposite = true;
                modifiedNodes.add(processedNode);
                for (Entry<String,JexlNode> usedAndedNode : eData.usedAndedNodes.entries())
                    if (leafNodes.containsEntry(usedAndedNode.getKey(), usedAndedNode.getValue()))
                        usedLeafNodes.put(usedAndedNode.getKey(), usedAndedNode.getValue());
                    else
                        parentData.usedAndedNodes.put(usedAndedNode.getKey(), usedAndedNode.getValue());
            } else
                unmodifiedNodes.add(nonLeafNode);
        }
        
        // add unmodified nodes, then modified nodes
        List<JexlNode> processedNodes = new ArrayList<>();
        processedNodes.addAll(unmodifiedNodes);
        processedNodes.addAll(modifiedNodes);
        
        return processedNodes;
    }
    
    /**
     * Attempts to create composites using the remaining leaf and anded nodes from our ancestors. Each of the composites created must contain at least one of
     * the leaf nodes in order to be valid. The used leaf nodes are passed back via the usedLeafNodes param. The used anded nodes are passed back via the
     * parentData.
     *
     * @param parentData
     *            Contains the ancestor anded nodes, anded nodes used to create the returned composites, and a flag indicating whether composites were found
     * @param leafNodes
     *            A multimap of leaf child nodes, keyed by field name, from the parent node
     * @param usedLeafNodes
     *            A multimap of used leaf child nodes, keyed by field name, used to create the returned composites
     * @return A list of modified and unmodified leaf child nodes, from the parent node
     */
    private List<JexlNode> processUnusedLeafNodes(ExpandData parentData, Multimap<String,JexlNode> leafNodes, Multimap<String,JexlNode> usedLeafNodes) {
        // use the remaining leaf and anded nodes to generate composites
        // note: the used leaf and anded nodes are removed in 'processNonLeafNodes'
        List<Composite> foundComposites = findComposites(leafNodes, parentData.andedNodes, usedLeafNodes, parentData.usedAndedNodes);
        
        List<JexlNode> compositeLeafNodes = new ArrayList<>();
        
        // if we found some composites
        if (!foundComposites.isEmpty()) {
            List<JexlNode> compositeNodes = createCompositeNodes(foundComposites);
            
            // add the composite nodes to our list of processed nodes
            if (!compositeNodes.isEmpty()) {
                parentData.foundComposite = true;
                
                compositeLeafNodes.add(createUnwrappedAndNode(compositeNodes));
            }
        }
        
        leafNodes.values().removeAll(usedLeafNodes.values());
        
        List<JexlNode> unmodifiedLeafNodes = new ArrayList<>();
        List<JexlNode> modifiedLeafNodes = new ArrayList<>();
        
        // finally, for any remaining leaf nodes at this level, visit
        // them, and add them to our list of processed nodes
        // note: if we encounter a composite here, it represents an
        // overloaded composite whose bounds were adjusted
        for (JexlNode remainingLeafNode : leafNodes.values()) {
            ExpandData eData = new ExpandData();
            JexlNode processedNode = (JexlNode) remainingLeafNode.jjtAccept(this, eData);
            
            if (eData.foundComposite) {
                parentData.foundComposite = true;
                modifiedLeafNodes.add(processedNode);
            } else {
                unmodifiedLeafNodes.add(remainingLeafNode);
            }
        }
        
        List<JexlNode> processedLeafNodes = new ArrayList<>();
        processedLeafNodes.addAll(unmodifiedLeafNodes);
        processedLeafNodes.addAll(modifiedLeafNodes);
        processedLeafNodes.addAll(compositeLeafNodes);
        
        return processedLeafNodes;
    }
    
    /**
     * Attempts to form jexl nodes from the composites
     * 
     * @param composites
     *            A list of composites from which jexl nodes should be created
     * @return A list of jexl nodes created from the given composites
     */
    private List<JexlNode> createCompositeNodes(List<Composite> composites) {
        List<JexlNode> nodeList = Lists.newArrayList();
        for (Composite comp : composites)
            nodeList.add(createCompositeNode(comp));
        return nodeList;
    }
    
    /**
     * Attempts to form a jexl node from the composite
     *
     * @param composite
     *            A list of composites from which jexl nodes should be created
     * @return A list of jexl nodes created from the given composite
     */
    private JexlNode createCompositeNode(Composite composite) {
        List<Class<? extends JexlNode>> nodeClasses = new ArrayList<>();
        List<String> appendedExpressions = new ArrayList<>();
        
        boolean includeOldData = false;
        if (config.getCompositeTransitionDates().containsKey(composite.compositeName)) {
            Date transitionDate = config.getCompositeTransitionDates().get(composite.compositeName);
            if (config.getBeginDate().compareTo(transitionDate) < 0)
                includeOldData = true;
        }
        
        composite.getNodesAndExpressions(nodeClasses, appendedExpressions, includeOldData);
        
        // if this is true, then it indicates that we are dealing with a query containing an overloaded composite
        // field which only contained the first component term. This means that we are running a query against
        // the base composite term, and thus need to expand our ranges to fully include both the composite and
        // non-composite events in our range.
        boolean expandRangeForBaseTerm = CompositeIngest.isOverloadedCompositeField(config.getCompositeToFieldMap(), composite.compositeName)
                        && composite.jexlNodeList.size() == 1;
        
        List<JexlNode> finalNodes = new ArrayList<>();
        for (int i = 0; i < nodeClasses.size(); i++) {
            Class<? extends JexlNode> nodeClass = nodeClasses.get(i);
            String appendedExpression = appendedExpressions.get(i);
            JexlNode newNode = null;
            if (nodeClass.equals(ASTGTNode.class)) {
                if (expandRangeForBaseTerm)
                    newNode = JexlNodeFactory.buildNode((ASTGENode) null, composite.compositeName, CompositeUtils.getInclusiveLowerBound(appendedExpression));
                else
                    newNode = JexlNodeFactory.buildNode((ASTGTNode) null, composite.compositeName, appendedExpression);
            } else if (nodeClass.equals(ASTGENode.class)) {
                newNode = JexlNodeFactory.buildNode((ASTGENode) null, composite.compositeName, appendedExpression);
            } else if (nodeClass.equals(ASTLTNode.class)) {
                newNode = JexlNodeFactory.buildNode((ASTLTNode) null, composite.compositeName, appendedExpression);
            } else if (nodeClass.equals(ASTLENode.class)) {
                if (expandRangeForBaseTerm)
                    newNode = JexlNodeFactory.buildNode((ASTLTNode) null, composite.compositeName, CompositeUtils.getExclusiveUpperBound(appendedExpression));
                else
                    newNode = JexlNodeFactory.buildNode((ASTLENode) null, composite.compositeName, appendedExpression);
            } else if (nodeClass.equals(ASTERNode.class)) {
                newNode = JexlNodeFactory.buildERNode(composite.compositeName, appendedExpression);
            } else if (nodeClass.equals(ASTNENode.class)) {
                newNode = JexlNodeFactory.buildNode((ASTNENode) null, composite.compositeName, appendedExpression);
            } else if (nodeClass.equals(ASTEQNode.class)) {
                // if this is for an overloaded composite field, which only includes the base term, convert to range
                if (expandRangeForBaseTerm) {
                    JexlNode lowerBound = JexlNodeFactory.buildNode((ASTGENode) null, composite.compositeName, appendedExpression);
                    JexlNode upperBound = JexlNodeFactory.buildNode((ASTLTNode) null, composite.compositeName,
                                    CompositeUtils.getExclusiveUpperBound(appendedExpression));
                    newNode = createUnwrappedAndNode(Arrays.asList(lowerBound, upperBound));
                } else {
                    newNode = JexlNodeFactory.buildEQNode(composite.compositeName, appendedExpression);
                }
            } else {
                log.error("Invalid or unknown node type for composite map.");
            }
            
            finalNodes.add(newNode);
        }
        
        JexlNode finalNode;
        if (finalNodes.size() > 1) {
            finalNode = createUnwrappedAndNode(finalNodes);
            if (composite.jexlNodeList.size() > 1) {
                JexlNode delayedNode = ASTDelayedPredicate.create(ASTCompositePredicate.create(createUnwrappedAndNode(composite.jexlNodeList.stream()
                                .map(node -> JexlNodeFactory.wrap(copy(node))).collect(Collectors.toList()))));
                finalNode = createUnwrappedAndNode(Arrays.asList(JexlNodeFactory.wrap(finalNode), delayedNode));
            }
        } else {
            finalNode = finalNodes.get(0);
            if (composite.jexlNodeList.size() > 1 && !(finalNode instanceof ASTEQNode)) {
                JexlNode delayedNode = ASTDelayedPredicate.create(ASTCompositePredicate.create(createUnwrappedAndNode(composite.jexlNodeList.stream()
                                .map(node -> JexlNodeFactory.wrap(copy(node))).collect(Collectors.toList()))));
                finalNode = createUnwrappedAndNode(Arrays.asList(finalNode, delayedNode));
            }
        }
        
        if (!CompositeIngest.isOverloadedCompositeField(config.getCompositeToFieldMap(), composite.compositeName)) {
            config.getIndexedFields().add(composite.compositeName);
            config.getQueryFieldsDatatypes().put(composite.compositeName, new NoOpType());
        }
        
        // save a mapping of generated composites to their component parts for later processing
        jexlNodeToCompMap.put(finalNode, composite);
        
        return finalNode;
    }
    
    private JexlNode visitLeafNode(JexlNode node, Object data) {
        if (data instanceof ExpandData)
            return visitLeafNode(node, (ExpandData) data);
        return node;
    }
    
    /**
     * The default leaf node visitor, which uses the anded nodes to determine whether a composite can be formed with this leaf node.
     *
     * @param node
     *            A leaf node from the original script
     * @param eData
     *            ExpandData, containing ancestor anded nodes, used anded nodes, and a flag indicating whether composites were found
     * @return Returns a composite node if one can be made, otherwise returns the original node
     */
    private JexlNode visitLeafNode(JexlNode node, ExpandData eData) {
        String fieldName = (node instanceof ASTAndNode) ? JexlASTHelper.getIdentifier(node.jjtGetChild(0)) : JexlASTHelper.getIdentifier(node);
        
        Multimap<String,JexlNode> leafNodes = LinkedHashMultimap.create();
        leafNodes.put(fieldName, node);
        
        List<Composite> foundComposites = findComposites(leafNodes, eData.andedNodes, eData.usedAndedNodes);
        
        JexlNode resultNode = node;
        
        // if composites were found, create JexlNodes from them
        if (!foundComposites.isEmpty()) {
            List<JexlNode> compositeNodes = createCompositeNodes(foundComposites);
            
            if (!compositeNodes.isEmpty()) {
                eData.foundComposite = true;
                resultNode = createUnwrappedAndNode(compositeNodes);
            }
        }
        
        return resultNode;
    }
    
    private List<Composite> findComposites(Multimap<String,JexlNode> leafNodes, Multimap<String,JexlNode> andedNodes, Multimap<String,JexlNode> usedAndedNodes) {
        return findComposites(leafNodes, andedNodes, null, usedAndedNodes);
    }
    
    /**
     * Using the leaf nodes and anded nodes passed in, attempts to create composites from those nodes. The generated composites are required to contain at least
     * one of the leaf nodes.
     *
     * @param leafNodes
     *            A multimap of leaf child nodes, keyed by field name, from the parent node
     * @param andedNodes
     *            A multimap of anded nodes, keyed by field name, passed down from our ancestors
     * @param usedLeafNodes
     *            A multimap of used leaf child nodes, keyed by field name, used to create the returned composites
     * @param usedAndedNodes
     *            A multimap of used anded nodes, keyed by field name, used to create the returned composites
     * @return A list of composites which can be created from the given leaf and anded nodes
     */
    private List<Composite> findComposites(Multimap<String,JexlNode> leafNodes, Multimap<String,JexlNode> andedNodes, Multimap<String,JexlNode> usedLeafNodes,
                    Multimap<String,JexlNode> usedAndedNodes) {
        
        // determine what composites can be made with these fields
        Multimap<String,String> filteredCompositeToFieldMap = getFilteredCompositeToFieldMap(leafNodes.keySet(), andedNodes.keySet());
        
        // TODO: Update this to use some kind of cardinality-based heuristic to sort the composites
        // order the composite to field map in order of preference
        filteredCompositeToFieldMap = orderByCollectionSize(filteredCompositeToFieldMap);
        
        // for each of the required fields, if is is an overloaded composite field,
        // we may need to create or tweak the leaf node's range, so we add a self-mapping
        for (String requiredField : leafNodes.keySet())
            if (CompositeIngest.isOverloadedCompositeField(config.getCompositeToFieldMap(), requiredField))
                filteredCompositeToFieldMap.put(requiredField, requiredField);
        
        return findComposites(filteredCompositeToFieldMap, leafNodes, andedNodes, usedLeafNodes, usedAndedNodes);
    }
    
    /**
     * Returns a map containing only the composites that could be created from the given set of required and other fields
     *
     * @param requiredFields
     *            A collection of fields, of which at least one must be present in each returned composite field mapping
     * @param otherFields
     *            A collection of other fields at our disposal for creating composites
     * @return A multimap of composite fields, and their component fields which can be created with the given fields
     */
    private Multimap<String,String> getFilteredCompositeToFieldMap(Collection<String> requiredFields, Collection<String> otherFields) {
        List<String> allFields = new ArrayList<>();
        allFields.addAll(requiredFields);
        allFields.addAll(otherFields);
        
        // determine which composites can be made
        Multimap<String,String> compositeToFieldMap = LinkedHashMultimap.create();
        for (String compositeField : config.getCompositeToFieldMap().keySet()) {
            Collection<String> componentFields = new ArrayList<>(config.getCompositeToFieldMap().get(compositeField));
            
            // determine whether one of our required fields is present
            boolean requiredFieldPresent = componentFields.stream().filter(fieldName -> requiredFields.contains(fieldName)).findAny().isPresent();
            
            // if a required field is present, and we have all of the
            // fields needed to make the composite, add it to our list
            if (requiredFieldPresent && allFields.containsAll(componentFields))
                compositeToFieldMap.putAll(compositeField, componentFields);
        }
        return compositeToFieldMap;
    }
    
    /**
     * Returns a list of composites that can be generated from the given leaf nodes and anded nodes. The used leaf nodes and anded nodes will be returned via
     * their respective parameters.
     *
     * @param filteredCompositeToFieldMap
     *            A multimap of composite fields, and their component fields which can be created with the given leaf and anded nodes
     * @param leafNodes
     *            A multimap of leaf child nodes, keyed by field name, from the parent node
     * @param andedNodes
     *            A multimap of anded nodes, keyed by field name, passed down from our ancestors
     * @param usedLeafNodes
     *            A multimap of used leaf child nodes, keyed by field name, used to create the returned composites
     * @param usedAndedNodes
     *            A multimap of used anded nodes, keyed by field name, used to create the returned composites
     * @return A list of composites which can be created from the given leaf and anded nodes
     */
    private List<Composite> findComposites(Multimap<String,String> filteredCompositeToFieldMap, Multimap<String,JexlNode> leafNodes,
                    Multimap<String,JexlNode> andedNodes, Multimap<String,JexlNode> usedLeafNodes, Multimap<String,JexlNode> usedAndedNodes) {
        List<Composite> composites = new ArrayList<>();
        
        // once a leaf node is used to create a composite, take it out of the rotation
        Multimap<String,JexlNode> remainingLeafNodes = LinkedHashMultimap.create();
        remainingLeafNodes.putAll(leafNodes);
        
        // once an anded node is used to create a composite, take it out of the rotation
        Multimap<String,JexlNode> remainingAndedNodes = LinkedHashMultimap.create();
        remainingAndedNodes.putAll(andedNodes);
        
        // look at each potential composite name to see if its fields are all available as keys in the childNodeMap
        for (Map.Entry<String,Collection<String>> compositeToFieldMap : filteredCompositeToFieldMap.asMap().entrySet()) {
            String compositeField = compositeToFieldMap.getKey();
            List<String> componentFields = new ArrayList<>(compositeToFieldMap.getValue());
            
            // is this a query against a composite field with old data whose date range predates the transition date?
            if (CompositeIngest.isOverloadedCompositeField(config.getCompositeToFieldMap(), compositeField)
                            && config.getCompositeTransitionDates().containsKey(compositeField)) {
                Date transitionDate = config.getCompositeTransitionDates().get(compositeField);
                if (config.getEndDate().compareTo(transitionDate) < 0)
                    continue;
            }
            
            // @formatter:off
            boolean leafNodeFieldPresent = componentFields.stream().
                    anyMatch(componentField -> remainingLeafNodes.keySet().contains(componentField));
            // @formatter:on
            
            // only build this composite if one of the components is a leaf node
            if (!leafNodeFieldPresent)
                continue;
            
            // @formatter:off
            boolean allRequiredFieldsPresent = !componentFields.stream().
                    anyMatch(componentField -> !(remainingLeafNodes.keySet().contains(componentField) || remainingAndedNodes.keySet().contains(componentField)));
            // @formatter:on
            
            // only build this composite if we have all of the required component fields
            if (!allRequiredFieldsPresent)
                continue;
            
            // we have what we need to make a composite
            List tempComposites = new ArrayList<>();
            Composite baseComp = new Composite(compositeField);
            tempComposites.add(baseComp);
            
            // keep track of the used nodes
            Multimap<String,JexlNode> tempUsedLeafNodes = LinkedHashMultimap.create();
            Multimap<String,JexlNode> tempUsedAndedNodes = LinkedHashMultimap.create();
            
            // traverse component nodes in order and add them to the
            // composite, creating additional nodes when necessary
            for (int i = 0; i < componentFields.size(); i++) {
                String componentField = componentFields.get(i);
                Collection nodes = Lists.newArrayList();
                
                // add any required leaf nodes
                for (JexlNode node : remainingLeafNodes.get(componentField)) {
                    JexlNode trimmedNode = getLeafNode(node);
                    if (trimmedNode != null && isNodeValid(trimmedNode, i, componentFields.size(), config.getFixedLengthFields().contains(componentField))) {
                        nodes.add(trimmedNode);
                        tempUsedLeafNodes.put(componentField, node);
                    }
                }
                
                // add any required anded nodes
                for (JexlNode node : remainingAndedNodes.get(componentField)) {
                    JexlNode trimmedNode = getLeafNode(node);
                    if (trimmedNode != null && isNodeValid(trimmedNode, i, componentFields.size(), config.getFixedLengthFields().contains(componentField))) {
                        nodes.add(trimmedNode);
                        tempUsedAndedNodes.put(componentField, node);
                    }
                }
                
                // if at any point we run out of eligible nodes, then we have failed to build the composite, and need to stop
                if (nodes.isEmpty()) {
                    tempComposites.clear();
                    break;
                }
                
                tempComposites = updateComposites(tempComposites, nodes);
                
                // if our updated composites list is empty,
                // then we failed to update the composites
                if (tempComposites.isEmpty())
                    break;
            }
            
            if (!tempComposites.isEmpty()) {
                // save the found composites
                composites.addAll(tempComposites);
                
                // keep track of the used nodes
                if (usedLeafNodes != null)
                    usedLeafNodes.putAll(tempUsedLeafNodes);
                if (usedAndedNodes != null)
                    usedAndedNodes.putAll(tempUsedAndedNodes);
                
                // take the used nodes out of the rotation
                for (Entry<String,JexlNode> usedNode : tempUsedLeafNodes.entries())
                    remainingLeafNodes.remove(usedNode.getKey(), usedNode.getValue());
                for (Entry<String,JexlNode> usedNode : tempUsedAndedNodes.entries())
                    remainingAndedNodes.remove(usedNode.getKey(), usedNode.getValue());
            }
        }
        
        return composites;
    }
    
    /**
     * This method is used to determine whether the given node will produce a valid composite in the given position, given the total number of fields in this
     * composite.
     *
     * @param node
     *            The node to check for validity
     * @param position
     *            The position this component node will occupy in the composite
     * @param numCompFields
     *            The number of component fields in the composite
     * @param isFixedLengthField
     *            Whether or not the field produces ranges over values of fixed length Used to determine whether this field can be used to generate a composite
     *            range
     * @return A boolean indicating whether the composite would be valid
     */
    private boolean isNodeValid(JexlNode node, int position, int numCompFields, boolean isFixedLengthField) {
        // if this is an equals node, the position doesn't matter
        if (node instanceof ASTEQNode) {
            return true;
        }
        // if this is a range node, and it is of fixed length, or it is the last field of the composite
        else if (node instanceof ASTAndNode && (isFixedLengthField || position == (numCompFields - 1))) {
            return true;
        }
        // if this is an unbounded range, or a regex node in the last position
        else if (node instanceof ASTGTNode || node instanceof ASTGENode || node instanceof ASTLTNode || node instanceof ASTLENode
                        || (node instanceof ASTERNode && position == (numCompFields - 1))) {
            return true;
        }
        return false;
    }
    
    /**
     * This method will add the given nodes to the passed in composites. If an invalid composite is produced as a result of adding in one of the nodes, we will
     * abort creation of this composite.
     *
     * @param composites
     *            Composite objects being tracked for a given key
     * @param nodes
     *            Collection of nodes matching a single field
     * @return A list of updated composites
     */
    private List<Composite> updateComposites(List<Composite> composites, Collection<JexlNode> nodes) {
        List<Composite> updatedComposites = new ArrayList<>();
        
        // add each of the nodes to each of the composites
        for (JexlNode node : nodes) {
            for (Composite composite : composites) {
                Composite updatedComposite;
                
                // at this point, an 'and' node represents a range
                if (node instanceof ASTAndNode)
                    updatedComposite = CompositeRange.clone(composite);
                else
                    updatedComposite = (nodes.size() > 1) ? composite : composite.clone();
                
                // update the composite
                addFieldToComposite(updatedComposite, node);
                
                // is it valid? if not, we can't build the composite
                if (!updatedComposite.isValid()) {
                    updatedComposites.clear();
                    break;
                }
                
                updatedComposites.add(updatedComposite);
            }
            
            // if we don't have any updated
            // composites, something went wrong
            if (updatedComposites.isEmpty())
                break;
        }
        
        return updatedComposites;
    }
    
    /**
     * Add Field to Composite object
     *
     * @param composite
     *            Composite nodes
     * @param node
     *            Jexl nodes to add to composite
     */
    private void addFieldToComposite(Composite composite, JexlNode node) {
        if (composite instanceof CompositeRange)
            addFieldToCompositeRange((CompositeRange) composite, node);
        else {
            Object lit = JexlASTHelper.getLiteralValue(node);
            String identifier = JexlASTHelper.getIdentifier(node);
            composite.jexlNodeList.add(node);
            composite.fieldNameList.add(identifier);
            composite.expressionList.add(lit.toString());
        }
    }
    
    /**
     * Add field to CompositeRange object
     *
     * @param composite
     *            Composite range node
     * @param node
     *            Jexl node to add to composite
     */
    private void addFieldToCompositeRange(CompositeRange composite, JexlNode node) {
        List<JexlNode> nodes = new ArrayList<>();
        if (node instanceof ASTAndNode) {
            nodes.addAll(Arrays.asList(children(node)));
        } else {
            nodes.add(node);
        }
        
        composite.jexlNodeList.add(node);
        
        for (int i = 0; i < nodes.size(); i++) {
            JexlNode theNode = nodes.get(i);
            Object lit = JexlASTHelper.getLiteralValue(theNode);
            String identifier = JexlASTHelper.getIdentifier(theNode);
            if (i == 0)
                composite.fieldNameList.add(identifier);
            if (theNode instanceof ASTGENode || theNode instanceof ASTGTNode) {
                composite.jexlNodeListLowerBound.add(theNode);
                composite.expressionListLowerBound.add(lit.toString());
                if (nodes.size() < 2) {
                    composite.jexlNodeListUpperBound.add(null);
                    composite.expressionListUpperBound.add(null);
                }
            } else if (theNode instanceof ASTLENode || theNode instanceof ASTLTNode) {
                composite.jexlNodeListUpperBound.add(theNode);
                composite.expressionListUpperBound.add(lit.toString());
                if (nodes.size() < 2) {
                    composite.jexlNodeListLowerBound.add(null);
                    composite.expressionListLowerBound.add(null);
                }
            } else if (theNode instanceof ASTEQNode) {
                composite.jexlNodeListLowerBound.add(theNode);
                composite.jexlNodeListUpperBound.add(theNode);
                composite.expressionListLowerBound.add(lit.toString());
                composite.expressionListUpperBound.add(lit.toString());
            }
        }
    }
    
    /**
     * This function is to order collections by size, this is useful so building composites can skip shorter composites ex. if KEY1_KEY2_KEY3 existed we would
     * not want to create KEY1_KEY2 or KEY1_KEY3 so could be skipped.
     * 
     * @param mm
     *            Multimap to sort by size of the {@code collection<v>}
     * @param <K>
     *            Key type for mm, not relevant to sort
     * @param <V>
     *            Value type for mm, not relevant to sort
     * @return Sorted linked hash multimap to keep order
     */
    private <K,V> LinkedHashMultimap<K,V> orderByCollectionSize(Multimap<K,V> mm) {
        List<Entry<K,Collection<V>>> orderedCompositeToFieldMap = new ArrayList<>(mm.asMap().entrySet());
        Collections.sort(orderedCompositeToFieldMap, (o1, o2) -> Integer.compare(o2.getValue().size(), o1.getValue().size()));
        
        LinkedHashMultimap<K,V> orderedMm = LinkedHashMultimap.create();
        for (Map.Entry<K,Collection<V>> foundEntry : orderedCompositeToFieldMap) {
            orderedMm.putAll(foundEntry.getKey(), foundEntry.getValue());
        }
        return orderedMm;
    }
    
    /**
     * This method checks each of the child nodes, and returns those which are leaf nodes. Range nodes are also considered leaf nodes for our purposes. If the
     * root node is a range node, then that node will be returned. Reference, ReferenceExpression, and 'and' or 'or' nodes with a single child are passed
     * through in search of the actual leaf node.
     *
     * @param rootNode
     *            The node whose children we will check
     * @param otherNodes
     *            Non-leaf child nodes of the root node
     * @return A multimap of field name to leaf node
     */
    private Multimap<String,JexlNode> getLeafNodes(JexlNode rootNode, Collection<JexlNode> otherNodes) {
        Multimap<String,JexlNode> childrenLeafNodes = ArrayListMultimap.create();
        
        if (rootNode instanceof ASTAndNode) {
            // check to see if this node is a range node, if so, this is our leaf node
            JexlNode leafKid = getLeafNode(rootNode);
            if (leafKid != null) {
                String kidFieldName;
                if (leafKid instanceof ASTAndNode) {
                    kidFieldName = JexlASTHelper.getIdentifier(leafKid.jjtGetChild(0));
                } else {
                    kidFieldName = JexlASTHelper.getIdentifier(leafKid);
                }
                childrenLeafNodes.put(kidFieldName, rootNode);
            }
        }
        
        if (childrenLeafNodes.isEmpty()) {
            for (JexlNode child : children(rootNode)) {
                JexlNode leafKid = getLeafNode(child);
                if (leafKid != null) {
                    String kidFieldName;
                    if (leafKid instanceof ASTAndNode) {
                        kidFieldName = JexlASTHelper.getIdentifier(leafKid.jjtGetChild(0));
                    } else {
                        kidFieldName = JexlASTHelper.getIdentifier(leafKid);
                    }
                    // note: we save the actual direct sibling of the and node, including
                    // any reference nodes. those will be trimmed off later
                    childrenLeafNodes.put(kidFieldName, child);
                } else {
                    if (otherNodes != null)
                        otherNodes.add(child);
                }
            }
        }
        return childrenLeafNodes;
    }
    
    /**
     * This method is used to find leaf nodes. Reference, ReferenceExpression, and 'and' or 'or' nodes with a single child are passed through in search of the
     * actual leaf node.
     *
     * @param node
     *            The node whose children we will check
     * @return The found leaf node, or null
     */
    private JexlNode getLeafNode(JexlNode node) {
        if (node instanceof ASTReference) {
            return getLeafNode((ASTReference) node);
        }
        
        if (node instanceof ASTReferenceExpression) {
            return getLeafNode((ASTReferenceExpression) node);
        }
        
        if (node instanceof ASTAndNode) {
            return getLeafNode((ASTAndNode) node);
        }
        
        if (CompositeUtils.VALID_LEAF_NODE_CLASSES.contains(node.getClass())) {
            return node;
        }
        
        return null;
    }
    
    /**
     * This method is used to find leaf nodes. Reference, ReferenceExpression, and 'and' or 'or' nodes with a single child are passed through in search of the
     * actual leaf node.
     *
     * @param node
     *            The node whose children we will check
     * @return The found leaf node, or null
     */
    private JexlNode getLeafNode(ASTReference node) {
        // ignore marked nodes
        if (!QueryPropertyMarkerVisitor.instanceOfAny(node)) {
            if (node.jjtGetNumChildren() == 1) {
                JexlNode kid = node.jjtGetChild(0);
                if (kid instanceof ASTReferenceExpression) {
                    return getLeafNode((ASTReferenceExpression) kid);
                }
            }
        }
        return null;
    }
    
    /**
     * This method is used to find leaf nodes. Reference, ReferenceExpression, and 'and' or 'or' nodes with a single child are passed through in search of the
     * actual leaf node.
     *
     * @param node
     *            The node whose children we will check
     * @return The found leaf node, or null
     */
    private JexlNode getLeafNode(ASTReferenceExpression node) {
        // ignore marked nodes
        if (!QueryPropertyMarkerVisitor.instanceOfAny(node)) {
            if (node instanceof ASTReferenceExpression && node.jjtGetNumChildren() == 1) {
                JexlNode kid = node.jjtGetChild(0);
                if (kid instanceof ASTAndNode) {
                    return getLeafNode((ASTAndNode) kid);
                } else if (CompositeUtils.VALID_LEAF_NODE_CLASSES.contains(kid.getClass())) {
                    return kid;
                } else {
                    return getLeafNode(kid);
                }
            }
        }
        
        return null;
    }
    
    /**
     * This method is used to find leaf nodes. Reference, ReferenceExpression, and 'and' or 'or' nodes with a single child are passed through in search of the
     * actual leaf node.
     *
     * @param node
     *            The node whose children we will check
     * @return The found leaf node, or null
     */
    private JexlNode getLeafNode(ASTAndNode node) {
        // ignore marked nodes
        if (!QueryPropertyMarkerVisitor.instanceOfAny(node)) {
            if (node.jjtGetNumChildren() == 1) {
                return getLeafNode(node.jjtGetChild(0));
            } else if (node.jjtGetNumChildren() == 2) {
                JexlNode beginNode = node.jjtGetChild(0);
                JexlNode endNode = node.jjtGetChild(1);
                if ((beginNode instanceof ASTGTNode || beginNode instanceof ASTGENode) && (endNode instanceof ASTLTNode || endNode instanceof ASTLENode)) {
                    String beginFieldName = JexlASTHelper.getIdentifier(beginNode);
                    String endFieldName = JexlASTHelper.getIdentifier(endNode);
                    if (beginFieldName.equals(endFieldName))
                        return node;
                }
            }
        }
        return null;
    }
    
    /**
     * This is a helper method which will attempt to create an 'and' node from the given jexl nodes. If a single node is passed, we will just return that node
     * instead of creating an unnecessary 'and' wrapper node.
     *
     * @param jexlNodes
     *            The nodes to 'and' together
     * @return An 'and' node comprised of the jexlNodes, or if a single jexlNode was passed in, we simply return that node.
     */
    private static JexlNode createUnwrappedAndNode(Collection<JexlNode> jexlNodes) {
        if (jexlNodes != null && !jexlNodes.isEmpty()) {
            if (jexlNodes.size() == 1)
                return jexlNodes.stream().findFirst().get();
            else
                return JexlNodeFactory.createUnwrappedAndNode(jexlNodes);
        }
        return null;
    }
    
    /**
     * This is a helper method which will attempt to create an 'or' node from the given jexl nodes. If a single node is passed, we will just return that node
     * instead of creating an unnecessary 'or' wrapper node.
     *
     * @param jexlNodes
     *            The nodes to 'or' together
     * @return An 'or' node comprised of the jexlNodes, or if a single jexlNode was passed in, we simply return that node.
     */
    private static JexlNode createUnwrappedOrNode(Collection<JexlNode> jexlNodes) {
        if (jexlNodes != null && !jexlNodes.isEmpty()) {
            if (jexlNodes.size() == 1)
                return jexlNodes.stream().findFirst().get();
            else
                return JexlNodeFactory.createUnwrappedOrNode(jexlNodes);
        }
        return null;
    }
    
    /**
     * Print Jexl node message /n node
     * 
     * @param queryTree
     * @param message
     */
    private void printJexlNode(JexlNode queryTree, String message) {
        printJexlNode(queryTree, message, Priority.toPriority(Level.TRACE_INT));
    }
    
    private void printJexlNode(JexlNode queryTree, String message, Priority priority) {
        if (log.isEnabledFor(priority)) {
            log.log(priority, message);
            for (String line : PrintingVisitor.formattedQueryStringList(queryTree)) {
                log.log(priority, line);
            }
        }
    }
    
    private void printWithMessage(String message, JexlNode node) {
        printWithMessage(message, node, Priority.toPriority(Level.TRACE_INT));
    }
    
    private void printWithMessage(String message, JexlNode node, Priority priority) {
        if (log.isEnabledFor(priority)) {
            log.log(priority, message + ":" + PrintingVisitor.formattedQueryString(node));
            log.log(priority, JexlStringBuildingVisitor.buildQuery(node));
        }
    }
    
    /**
     * This is a visitor which is used to fully distribute anded nodes into a given node. The visitor will only distribute the anded nodes to those descendant
     * nodes within the tree with which they are not already anded (via a composite).
     *
     */
    private static class DistributeAndedNodes extends RebuildingVisitor {
        private JexlNode initialNode = null;
        private List<JexlNode> andedNodes;
        private Map<JexlNode,Composite> compositeNodes;
        
        private static class DistAndData {
            Set<JexlNode> usedAndedNodes = new HashSet<>();
        }
        
        private DistributeAndedNodes(List<JexlNode> andedNodes, Map<JexlNode,Composite> compositeNodes) {
            this.andedNodes = andedNodes;
            this.compositeNodes = compositeNodes;
        }
        
        /**
         * Distribute the anded node, making sure to 'and' it in at the highest possible level of the tree. This version takes a map of composite nodes to their
         * component nodes, so that we can better check composite nodes to see if they already include the anded node. That is to say, we will not 'and' a
         * composite node with one of it's component nodes.
         *
         * @param script
         *            The node that we will be distributing the anded nodes into
         * @param andedNodes
         *            The nodes which we will be distributing into the root node
         * @param compositeNodes
         *            A map of generated composite jexl nodes to the composite object used to create that node
         * @return An updated script with the anded nodes distributed throughout
         */
        @SuppressWarnings("unchecked")
        public static JexlNode distributeAndedNode(JexlNode script, List<JexlNode> andedNodes, Map<JexlNode,Composite> compositeNodes) {
            DistributeAndedNodes visitor = new DistributeAndedNodes(andedNodes, compositeNodes);
            DistAndData foundData = new DistAndData();
            JexlNode resultNode = (JexlNode) script.jjtAccept(visitor, foundData);
            
            if (!foundData.usedAndedNodes.containsAll(andedNodes)) {
                List<JexlNode> nodes = new ArrayList<>();
                nodes.addAll(andedNodes.stream().filter(node -> !foundData.usedAndedNodes.contains(node)).map(RebuildingVisitor::copy)
                                .collect(Collectors.toList()));
                nodes.add(resultNode);
                
                return createUnwrappedAndNode(nodes);
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
            DistAndData parentData = (DistAndData) data;
            
            if (initialNode == null || initialNode instanceof ASTReference || initialNode instanceof ASTReferenceExpression)
                initialNode = node;
            
            // if this node is one of the anded nodes, or a composite
            // comprised of one of the anded nodes, halt recursion
            List<JexlNode> usedAndedNodes = usesAndedNodes(node);
            if (!usedAndedNodes.isEmpty()) {
                parentData.usedAndedNodes.addAll(usedAndedNodes);
                return node;
            }
            
            // don't descend into composite nodes, and don't copy them
            // this logic is dependent upon identifying composite nodes by their address
            if (compositeNodes.containsKey(node)) {
                return node;
            }
            
            boolean rebuildNode = false;
            
            // check each child node
            List<JexlNode> nodesMissingEverything = new ArrayList<>();
            List<JexlNode> nodesWithEverything = new ArrayList<>();
            Map<JexlNode,List<JexlNode>> nodesMissingSomething = new LinkedHashMap<>();
            for (JexlNode child : children(node)) {
                DistAndData foundData = new DistAndData();
                JexlNode processedChild = (JexlNode) child.jjtAccept(this, foundData);
                
                if (processedChild != child)
                    rebuildNode = true;
                
                if (foundData.usedAndedNodes.isEmpty())
                    nodesMissingEverything.add(processedChild);
                else if (!foundData.usedAndedNodes.containsAll(andedNodes)) {
                    List<JexlNode> missingAndedNodes = new ArrayList<>();
                    missingAndedNodes.addAll(andedNodes);
                    missingAndedNodes.removeAll(foundData.usedAndedNodes);
                    nodesMissingSomething.put(processedChild, missingAndedNodes);
                } else
                    nodesWithEverything.add(processedChild);
            }
            
            // if none of the children are missing anything, we're done
            if (nodesWithEverything.size() == node.jjtGetNumChildren()) {
                parentData.usedAndedNodes.addAll(andedNodes);
                if (rebuildNode)
                    return createUnwrappedOrNode(nodesWithEverything);
                else
                    return node;
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
            for (Entry<JexlNode,List<JexlNode>> childEntry : nodesMissingSomething.entrySet())
                rebuiltChildren.add(DistributeAndedNodes.distributeAndedNode(childEntry.getKey(), childEntry.getValue(), compositeNodes));
            
            // for children missing everything -> 'or' them together, then 'and' them with the full set of andedNodes
            List<JexlNode> nodeList = andedNodes.stream().map(RebuildingVisitor::copy).collect(Collectors.toList());
            
            nodeList.add(createUnwrappedOrNode(nodesMissingEverything));
            
            rebuiltChildren.add(createUnwrappedAndNode(nodeList));
            
            // for children with everything -> keep those as-is
            rebuiltChildren.addAll(nodesWithEverything);
            
            parentData.usedAndedNodes.addAll(andedNodes);
            
            return createUnwrappedOrNode(rebuiltChildren);
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
            DistAndData parentData = (DistAndData) data;
            
            if (initialNode == null || initialNode instanceof ASTReference || initialNode instanceof ASTReferenceExpression)
                initialNode = node;
            
            // if this node is one of the anded nodes, or a composite
            // comprised of one of the anded nodes, halt recursion
            List<JexlNode> usedAndedNodes = usesAndedNodes(node);
            if (!usedAndedNodes.isEmpty()) {
                parentData.usedAndedNodes.addAll(usedAndedNodes);
                return node;
            }
            
            // don't descend into composite nodes, and don't copy them
            // this logic is dependent upon identifying composite nodes by their address
            if (compositeNodes.containsKey(node)) {
                return node;
            }
            
            // check each child node to see how many of the desired andedNodes are present
            List<JexlNode> rebuiltChildren = new ArrayList<>();
            for (JexlNode child : children(node)) {
                DistAndData foundData = new DistAndData();
                rebuiltChildren.add((JexlNode) child.jjtAccept(this, foundData));
                
                parentData.usedAndedNodes.addAll(foundData.usedAndedNodes);
            }
            
            // are some anded nodes missing, and is this the initial node?
            if (!parentData.usedAndedNodes.containsAll(andedNodes) && node.equals(initialNode)) {
                // 'and' with the missing anded nodes, and return
                List<JexlNode> nodes = new ArrayList<>();
                nodes.addAll(andedNodes.stream().filter(andedNode -> !parentData.usedAndedNodes.contains(andedNode)).map(RebuildingVisitor::copy)
                                .collect(Collectors.toList()));
                nodes.add(node);
                
                // this is probably unnecessary, but to be safe, let's set it
                parentData.usedAndedNodes.addAll(andedNodes);
                
                return createUnwrappedAndNode(nodes);
            }
            
            return createUnwrappedAndNode(rebuiltChildren);
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
        public Object visit(ASTReference node, Object data) {
            visitInternal(node, data);
            return super.visit(node, data);
        }
        
        @Override
        public Object visit(ASTReferenceExpression node, Object data) {
            visitInternal(node, data);
            return super.visit(node, data);
        }
        
        /**
         * Used to determine whether this is a composite node, and if so, which of the anded nodes does it have as components
         *
         * @param node
         *            The node to check for anded components
         * @return A list of anded jexl nodes used to create the composite node
         */
        private List<JexlNode> usesAndedNodes(JexlNode node) {
            List<JexlNode> usedAndedNodes = new ArrayList<>();
            for (JexlNode andedNode : andedNodes)
                if (compositeNodes.containsKey(node) && compositeNodes.get(node).contains(andedNode))
                    usedAndedNodes.add(andedNode);
            return usedAndedNodes;
        }
        
        private void visitInternal(JexlNode node, Object data) {
            if (initialNode == null || initialNode instanceof ASTReference || initialNode instanceof ASTReferenceExpression)
                initialNode = node;
            
            DistAndData parentData = (DistAndData) data;
            parentData.usedAndedNodes.addAll(usesAndedNodes(node));
        }
    }
}
