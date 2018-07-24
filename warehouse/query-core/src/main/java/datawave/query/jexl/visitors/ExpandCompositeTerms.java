package datawave.query.jexl.visitors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.data.type.NoOpType;
import datawave.ingest.data.config.ingest.CompositeIngest;
import datawave.query.composite.Composite;
import datawave.query.composite.CompositeRange;
import datawave.query.composite.CompositeUtils;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.commons.collections.keyvalue.DefaultMapEntry;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTCompositePredicate;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class ExpandCompositeTerms extends RebuildingVisitor {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(ExpandCompositeTerms.class);
    
    private final ShardQueryConfiguration config;
    
    private static class ExpandData {
        public JexlNode root;
        public Collection<JexlNode> goners;
        public Multimap<String,JexlNode> andEqualsNodes;
        public LinkedHashMultimap<String,String> orderedCompositeToFieldMap;
        public List<JexlNode> newNodes;
        
        /**
         * @param root
         *            The node that kicked off visitor
         * @param andEqualsNodes
         *            And equals nodes from ancestors
         * @param orderedCompositeToFieldMap
         *            Composite field map ordered from largest number of fields to least
         * @param goners
         *            Collection of nodes to be removed after finished
         */
        ExpandData(JexlNode root, Multimap<String,JexlNode> andEqualsNodes, LinkedHashMultimap<String,String> orderedCompositeToFieldMap,
                        Collection<JexlNode> goners) {
            this.root = root;
            this.andEqualsNodes = andEqualsNodes;
            this.orderedCompositeToFieldMap = orderedCompositeToFieldMap;
            this.goners = goners;
            this.newNodes = Lists.newArrayList();
        }
    }
    
    private ExpandCompositeTerms(ShardQueryConfiguration config) {
        Preconditions.checkNotNull(config);
        this.config = config;
    }
    
    /**
     * Expand all nodes which have multiple dataTypes for the field.
     *
     * @param config
     * @param script
     * @return
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
        
        return (T) script.jjtAccept(visitor, null);
    }
    
    public Object visit(ASTNotNode node, Object data) {
        // do not descend down not nodes
        if (data instanceof ExpandData) {
            return node;
        }
        
        // return as normal
        return super.visit(node, data);
    }
    
    /**
     * 
     * @param node
     * @param data
     * @return
     */
    @Override
    public Object visit(ASTOrNode node, Object data) {
        // Or nodes only valid under "AND" node so previous data is needed
        if (data == null || !(data instanceof ExpandData)) {
            return super.visit(node, data);
        }
        
        ExpandData eData = (ExpandData) data;
        List<JexlNode> kidOtherNodes = Lists.newArrayList();
        List<JexlNode> descendantCompositeNodes = Lists.newArrayList();
        if (node != null) {
            Multimap<String,Composite> foundCompositeMaps = getFoundCompositeMap(node, eData.andEqualsNodes, eData.orderedCompositeToFieldMap, eData.goners,
                            kidOtherNodes, descendantCompositeNodes);
            
            JexlNode rebuiltChildOr = rebuildOrNode(foundCompositeMaps, kidOtherNodes, descendantCompositeNodes, eData.goners);
            
            if (rebuiltChildOr != null) {
                this.printJexlNode(rebuiltChildOr, "New orNode: ");
                
                eData.goners.add(eData.root);
                this.printWithMessage("added to goners:", eData.root);
                
                eData.newNodes.add(rebuiltChildOr);
                return rebuiltChildOr;
            }
        }
        
        return super.visit(node, data);
    }
    
    /**
     *
     * @param node
     * @param eData
     * @return
     */
    public JexlNode visit(ASTAndNode node, ExpandData eData) {
        if (node != null) {
            List<JexlNode> kidOtherNodes = Lists.newArrayList();
            List<JexlNode> descendantCompositeNodes = Lists.newArrayList();
            Multimap<String,Composite> foundCompositeMaps = getFoundCompositeMap(node, eData.andEqualsNodes, eData.orderedCompositeToFieldMap, eData.goners,
                            kidOtherNodes, descendantCompositeNodes);
            
            if (log.isDebugEnabled()) {
                log.debug("Found Composite Map: " + foundCompositeMaps);
            }
            
            JexlNode rebuiltAndChild = rebuildAndNode(foundCompositeMaps, kidOtherNodes, descendantCompositeNodes, eData.goners);
            
            if (rebuiltAndChild != null) {
                this.printJexlNode(rebuiltAndChild, "New AndNode: ");
                
                eData.goners.add(eData.root);
                this.printWithMessage("added to goners:", eData.root);
                
                eData.newNodes.add(rebuiltAndChild);
            }
            
            return rebuiltAndChild;
        }
        return null;
    }
    
    /**
     * If no composites can be formed, then process this node normally (return a copy of the incoming node)
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // Add and child nodes
        if (data != null && data instanceof ExpandData) {
            return this.visit(node, (ExpandData) data);
        }
        
        Set<String> componentFields = new HashSet<>(config.getCompositeToFieldMap().values());
        Collection<JexlNode> andChildrenGoners = Sets.newHashSet();
        boolean hasEqNode = false;
        for (JexlNode kid : JexlNodes.children(node)) {
            while ((kid instanceof ASTReference || kid instanceof ASTReferenceExpression || kid instanceof ASTAndNode) && kid.jjtGetNumChildren() == 1)
                kid = kid.jjtGetChild(0);
            if (CompositeUtils.LEAF_NODE_CLASSES.contains(kid.getClass()) && componentFields.contains(JexlASTHelper.getIdentifier(kid))) {
                hasEqNode = true;
                break;
            } else if (kid instanceof ASTAndNode) {
                List<JexlNode> others = new ArrayList<>();
                Map<LiteralRange<?>,List<JexlNode>> boundedRangesIndexAgnostic = JexlASTHelper.getBoundedRangesIndexAgnostic(kid, others, true, 1);
                for (LiteralRange range : boundedRangesIndexAgnostic.keySet()) {
                    if (componentFields.contains(range.getFieldName())) {
                        hasEqNode = true;
                        break;
                    }
                }
                if (hasEqNode)
                    break;
            }
        }
        
        if (!hasEqNode) {
            return super.visit(node, data); // no equal nodes to build
        }
        
        Multimap<String,String> compositeToFieldMap = config.getCompositeToFieldMap();
        LinkedHashMultimap<String,String> orderedCompositeToFieldMap = orderByCollectionSize(compositeToFieldMap);
        
        Multimap<String,JexlNode> andChildrenEqualsNodes = ArrayListMultimap.create(); // all the ASTEQNodes
        
        printWithMessage("Looking for composite keys in: ", node);
        ExpandData nodeData = new ExpandData(node, andChildrenEqualsNodes, orderedCompositeToFieldMap, andChildrenGoners);
        JexlNode andNode = this.visit(node, nodeData);
        if (andNode == null) {
            return super.visit(node, data); // got no composites, carry on
        }
        
        this.printJexlNode(andNode, "And here's my new and node:");
        return andNode;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (data == null) {
            String fieldName = JexlASTHelper.getIdentifier(node);
            if (fieldName != null && config.getCompositeToFieldMap().containsKey(fieldName)) {
                Object expression = JexlASTHelper.getLiteralValue(node);
                // if this is an overloaded composite field, convert ASTEQNode to range
                if (expression != null && CompositeIngest.isOverloadedCompositeField(config.getCompositeToFieldMap(), fieldName)) {
                    JexlNode lowerBound = JexlNodeFactory.buildNode((ASTGENode) null, fieldName, expression.toString());
                    JexlNode upperBound = JexlNodeFactory.buildNode((ASTLTNode) null, fieldName, CompositeUtils.getExclusiveUpperBound(expression.toString()));
                    return JexlNodeFactory.createAndNode(Arrays.asList(lowerBound, upperBound));
                }
            }
        }
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        if (data == null) {
            String fieldName = JexlASTHelper.getIdentifier(node);
            if (fieldName != null && config.getCompositeToFieldMap().containsKey(fieldName)) {
                Object expression = JexlASTHelper.getLiteralValue(node);
                if (expression != null && CompositeIngest.isOverloadedCompositeField(config.getCompositeToFieldMap(), fieldName)) {
                    return JexlNodeFactory.buildNode((ASTGENode) null, fieldName, CompositeUtils.getInclusiveLowerBound(expression.toString()));
                }
            }
        }
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        if (data == null) {
            String fieldName = JexlASTHelper.getIdentifier(node);
            if (fieldName != null && config.getCompositeToFieldMap().containsKey(fieldName)) {
                Object expression = JexlASTHelper.getLiteralValue(node);
                if (expression != null && CompositeIngest.isOverloadedCompositeField(config.getCompositeToFieldMap(), fieldName)) {
                    return JexlNodeFactory.buildNode((ASTLTNode) null, fieldName, CompositeUtils.getExclusiveUpperBound(expression.toString()));
                }
            }
        }
        return super.visit(node, data);
    }
    
    /**
     *
     * @param foundCompositeMaps
     * @param otherNodes
     * @param descendantCompositeNodes
     * @param goners
     * @return
     */
    private JexlNode rebuildOrNode(Multimap<String,Composite> foundCompositeMaps, Collection<JexlNode> otherNodes,
                    Collection<JexlNode> descendantCompositeNodes, Collection<JexlNode> goners) {
        List<JexlNode> nodeList = getNodeList(foundCompositeMaps);
        if (nodeList.size() > 0 || descendantCompositeNodes.size() > 0) {
            nodeList.addAll(otherNodes);
            nodeList.addAll(descendantCompositeNodes);
            nodeList.removeAll(goners);
            
            JexlNode orNode;
            // if there's more than one node, wrap any and nodes
            if (nodeList.size() > 1) {
                for (int i = 0; i < nodeList.size(); i++)
                    if (nodeList.get(i) instanceof ASTAndNode)
                        nodeList.set(i, JexlNodeFactory.wrap(nodeList.get(i)));
                orNode = JexlNodeFactory.createOrNode(nodeList);
            } else
                orNode = nodeList.get(0);
            
            if (log.isTraceEnabled()) {
                PrintingVisitor.printQuery(orNode);
            }
            return orNode;
        }
        return null;
    }
    
    private JexlNode rebuildAndNode(Multimap<String,Composite> foundCompositeMaps, List<JexlNode> otherNodes, Collection<JexlNode> descendantCompositeNodes,
                    Collection<JexlNode> goners) {
        List<JexlNode> nodeList = getNodeList(foundCompositeMaps);
        if ((nodeList != null && !nodeList.isEmpty()) || (descendantCompositeNodes != null && !descendantCompositeNodes.isEmpty())) {
            nodeList.addAll(otherNodes);
            nodeList.addAll(descendantCompositeNodes);
            nodeList.removeAll(goners);
            
            JexlNode andNode;
            // if there's more than one node, wrap any and nodes
            if (nodeList.size() > 1) {
                for (int i = 0; i < nodeList.size(); i++)
                    if (nodeList.get(i) instanceof ASTAndNode)
                        nodeList.set(i, JexlNodeFactory.wrap(nodeList.get(i)));
                andNode = JexlNodeFactory.createAndNode(nodeList);
            } else
                andNode = nodeList.get(0);
            
            if (log.isTraceEnabled()) {
                PrintingVisitor.printQuery(andNode);
            }
            return andNode;
        }
        return null;
    }
    
    /**
     * Forms a jexl node for each found composite map and returns it as a list
     * 
     * @param foundCompositeMaps
     *            {@code Composite Name -> Collection<Composites>}
     * @return List of composite nodes
     */
    private List<JexlNode> getNodeList(Multimap<String,Composite> foundCompositeMaps) {
        List<JexlNode> nodeList = Lists.newArrayList();
        for (Composite comp : foundCompositeMaps.values()) {
            List<JexlNode> nodes = new ArrayList<>();
            List<String> appendedExpressions = new ArrayList<>();
            
            boolean includeOldData = false;
            if (config.getCompositeTransitionDates().containsKey(comp.compositeName)) {
                Date transitionDate = config.getCompositeTransitionDates().get(comp.compositeName);
                if (config.getBeginDate().compareTo(transitionDate) < 0)
                    includeOldData = true;
            }
            
            comp.getNodesAndExpressions(nodes, appendedExpressions, includeOldData);
            
            // if this is true, then it indicates that we are dealing with a query containing an overloaded composite
            // field which only contained the first component term. This means that we are running a query against
            // the base composite term, and thus need to expand our ranges to fully include both the composite and
            // non-composite events in our range.
            boolean expandRangeForBaseTerm = CompositeIngest.isOverloadedCompositeField(config.getCompositeToFieldMap(), comp.compositeName)
                            && comp.jexlNodeList.size() == 1;
            
            List<JexlNode> finalNodes = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i++) {
                JexlNode node = nodes.get(i);
                String appendedExpression = appendedExpressions.get(i);
                JexlNode newNode = null;
                if (node instanceof ASTGTNode) {
                    if (expandRangeForBaseTerm)
                        newNode = JexlNodeFactory.buildNode((ASTGENode) null, comp.compositeName, CompositeUtils.getInclusiveLowerBound(appendedExpression));
                    else
                        newNode = JexlNodeFactory.buildNode((ASTGTNode) null, comp.compositeName, appendedExpression);
                } else if (node instanceof ASTGENode) {
                    newNode = JexlNodeFactory.buildNode((ASTGENode) null, comp.compositeName, appendedExpression);
                } else if (node instanceof ASTLTNode) {
                    newNode = JexlNodeFactory.buildNode((ASTLTNode) null, comp.compositeName, appendedExpression);
                } else if (node instanceof ASTLENode) {
                    if (expandRangeForBaseTerm)
                        newNode = JexlNodeFactory.buildNode((ASTLTNode) null, comp.compositeName, CompositeUtils.getExclusiveUpperBound(appendedExpression));
                    else
                        newNode = JexlNodeFactory.buildNode((ASTLENode) null, comp.compositeName, appendedExpression);
                } else if (node instanceof ASTERNode) {
                    newNode = JexlNodeFactory.buildERNode(comp.compositeName, appendedExpression);
                } else if (node instanceof ASTNENode) {
                    newNode = JexlNodeFactory.buildNode((ASTNENode) null, comp.compositeName, appendedExpression);
                } else if (node instanceof ASTEQNode) {
                    // if this is for an overloaded composite field, which only includes the base term, convert to range
                    if (expandRangeForBaseTerm) {
                        JexlNode lowerBound = JexlNodeFactory.buildNode((ASTGENode) null, comp.compositeName, appendedExpression);
                        JexlNode upperBound = JexlNodeFactory.buildNode((ASTLTNode) null, comp.compositeName,
                                        CompositeUtils.getExclusiveUpperBound(appendedExpression));
                        newNode = JexlNodeFactory.createAndNode(Arrays.asList(lowerBound, upperBound));
                    } else {
                        newNode = JexlNodeFactory.buildEQNode(comp.compositeName, appendedExpression);
                    }
                } else {
                    log.error("Invalid or unknown node type for composite map.");
                }
                
                finalNodes.add(newNode);
            }
            
            if (finalNodes.size() > 1) {
                JexlNode finalNode = JexlNodeFactory.createUnwrappedAndNode(finalNodes);
                if (comp.jexlNodeList.size() > 1) {
                    JexlNode delayedNode = ASTDelayedPredicate.create(ASTCompositePredicate.create(JexlNodeFactory.createUnwrappedAndNode(comp.jexlNodeList
                                    .stream().map(node -> JexlNodeFactory.wrap(node)).collect(Collectors.toList()))));
                    finalNode = JexlNodeFactory.createUnwrappedAndNode(Arrays.asList(JexlNodeFactory.wrap(finalNode), delayedNode));
                }
                nodeList.add(finalNode);
            } else {
                JexlNode finalNode = finalNodes.get(0);
                if (comp.jexlNodeList.size() > 1 && !(finalNode instanceof ASTEQNode)) {
                    JexlNode delayedNode = ASTDelayedPredicate.create(ASTCompositePredicate.create(JexlNodeFactory.createUnwrappedAndNode(comp.jexlNodeList
                                    .stream().map(node -> JexlNodeFactory.wrap(node)).collect(Collectors.toList()))));
                    finalNode = JexlNodeFactory.createUnwrappedAndNode(Arrays.asList(finalNode, delayedNode));
                }
                nodeList.add(finalNode);
            }
            
            if (!CompositeIngest.isOverloadedCompositeField(config.getCompositeToFieldMap(), comp.compositeName)) {
                config.getIndexedFields().add(comp.compositeName);
                config.getQueryFieldsDatatypes().put(comp.compositeName, new NoOpType());
            }
        }
        return nodeList;
    }
    
    /**
     *
     * @param child
     * @param andChildrenEqualsNodes
     * @param orderedCompositeToFieldMap
     * @param childrenGoners
     * @param otherNodes
     * @return found composite nodes
     */
    private Multimap<String,Composite> getFoundCompositeMap(ASTAndNode child, Multimap<String,JexlNode> andChildrenEqualsNodes,
                    LinkedHashMultimap<String,String> orderedCompositeToFieldMap, Collection<JexlNode> childrenGoners, Collection<JexlNode> otherNodes,
                    Collection<JexlNode> descendantCompositeNodes) {
        Multimap<String,JexlNode> childrenLeafNodes = getChildLeafNodes(child, otherNodes);
        searchDescendants(andChildrenEqualsNodes, orderedCompositeToFieldMap, childrenGoners, otherNodes, descendantCompositeNodes, childrenLeafNodes);
        
        Multimap<String,Composite> leafFound = getFoundCompositeMapAnd(andChildrenEqualsNodes, childrenLeafNodes, orderedCompositeToFieldMap);
        fixAndGoners(leafFound, andChildrenEqualsNodes, childrenGoners);
        
        cleanOtherNodes(childrenLeafNodes, otherNodes);
        
        return leafFound;
    }
    
    private Multimap<String,Composite> getFoundCompositeMap(ASTOrNode child, Multimap<String,JexlNode> andChildrenEqualsNodes,
                    LinkedHashMultimap<String,String> orderedCompositeToFieldMap, Collection<JexlNode> childrenGoners, Collection<JexlNode> otherNodes,
                    Collection<JexlNode> descendantCompositeNodes) {
        Multimap<String,JexlNode> childrenLeafNodes = getChildLeafNodes(child, otherNodes);
        searchDescendants(andChildrenEqualsNodes, orderedCompositeToFieldMap, childrenGoners, otherNodes, descendantCompositeNodes, null);
        
        Multimap<String,Composite> leafFound = getFoundCompositeMapOr(andChildrenEqualsNodes, childrenLeafNodes, orderedCompositeToFieldMap);
        fixOrGoners(leafFound, andChildrenEqualsNodes, childrenGoners);
        
        cleanOtherNodes(childrenLeafNodes, otherNodes);
        
        // Other nodes exist restore all and goners
        if (otherNodes.size() > 0 && !childrenGoners.isEmpty()) {
            for (Entry<String,JexlNode> andChildEqualsNode : andChildrenEqualsNodes.entries()) {
                childrenGoners.remove(andChildEqualsNode.getValue());
            }
        }
        
        return leafFound;
    }
    
    /**
     * Form the composite map based on child leaf nodes and available composite keys
     *
     * @param andChildNodeMap
     *            Ancestors "and" nodes
     * @param leafNodeMap
     *            {@code Leaf nodes, List<JexlNode> = bounded range; JexlNode = EqNode, ErNode, NeNode}
     * @param compositeToFieldMap
     *            Composite fields ordered from most fields to least
     * @return valid matched composite indexes
     */
    private Multimap<String,Composite> getFoundCompositeMapAnd(Multimap<String,JexlNode> andChildNodeMap, Multimap<String,JexlNode> leafNodeMap,
                    Multimap<String,String> compositeToFieldMap) {
        
        Multimap<String,Composite> foundCompositeMap = ArrayListMultimap.create();
        
        // if this is an overloaded composite field, we need to also add a mapping to itself in order to ensure
        // that the ranges are expanded to include composite and non-composite terms. We add this as the last
        // composite mapping to ensure that it is only created in the event that no other composites can be formed.
        Set<Entry<String,Collection<String>>> overloadedEntries = new HashSet<>();
        for (Map.Entry<String,Collection<String>> entry : compositeToFieldMap.asMap().entrySet()) {
            if (CompositeIngest.isOverloadedCompositeField(entry.getValue(), entry.getKey()))
                overloadedEntries.add(new DefaultMapEntry(entry.getKey(), Arrays.asList(entry.getKey())));
        }
        
        for (Set<Entry<String,Collection<String>>> entries : new Set[] {compositeToFieldMap.asMap().entrySet(), overloadedEntries}) {
            // look at each potential composite name to see if its fields are all available as keys in the childNodeMap
            for (Map.Entry<String,Collection<String>> entry : entries) {
                // Is this a query against a composite field with old data whose date range predates the transition date?
                if (isQueryAgainstCompositeWithOldDataPriorToTransitionDate(entry.getKey()))
                    continue;
                
                // Did we find a larger key containing this one
                if (isCompositeFieldContainedInFoundMapWithList(foundCompositeMap, entry.getValue()))
                    continue;
                
                // Is a child node part of the key?
                if (containsAnyCompositeNodes(entry.getValue(), leafNodeMap.keySet()) == false)
                    continue;
                
                // first see if valid fields for the composite are available in the 'or' and 'and' nodes
                Set<String> leafNodeKeySet = (leafNodeMap != null) ? Sets.newHashSet(leafNodeMap.keySet()) : null;
                Set<String> andChildKeySet = (andChildNodeMap != null) ? andChildNodeMap.keySet() : null;
                if (this.containsAllCompositeNodes(entry.getValue(), leafNodeKeySet, andChildKeySet) == false)
                    continue;
                
                // i can make a composite....
                // the entry.value() collection is sorted in the correct order for the fields
                String compositeName = entry.getKey();
                List<Composite> comps = Lists.newArrayList();
                Composite baseComp = new Composite(compositeName);
                comps.add(baseComp);
                List<String> compositeFields = new ArrayList<>(entry.getValue());
                Multimap<String,JexlNode> usedLeafNodes = HashMultimap.create();
                for (int i = 0; i < compositeFields.size(); i++) {
                    String compField = compositeFields.get(i);
                    Collection nodes = Lists.newArrayList();
                    
                    if (leafNodeMap != null) {
                        for (JexlNode node : leafNodeMap.get(compField)) {
                            if (isNodeValid(node, i, compositeFields.size(), config.getFixedLengthFields().contains(compField))) {
                                nodes.add(node);
                                usedLeafNodes.put(compField, node);
                            }
                        }
                    }
                    
                    if (andChildNodeMap != null) {
                        for (JexlNode node : andChildNodeMap.get(compField))
                            if (isNodeValid(node, i, compositeFields.size(), config.getFixedLengthFields().contains(compField)))
                                nodes.add(node);
                    }
                    
                    // if at any point we run out of eligible nodes, then we have failed to build the composite, and need to stop
                    if (nodes.isEmpty()) {
                        comps = null;
                        break;
                    }
                    
                    boolean wildCardFound = updateComposites(comps, nodes);
                    if (wildCardFound) {
                        // Wild card can introduce single value comp keys and comps with out leaf nodes
                        cleanInValidCompositeNodes(comps, usedLeafNodes);
                        break;
                    }
                }
                
                // remove all of the leaf nodes that were successfully used, but if they were used
                // to make a composite range, or an unbounded composite, make sure they are not goners
                for (Entry<String,JexlNode> usedLeafEntry : usedLeafNodes.entries()) {
                    String compName = usedLeafEntry.getKey();
                    JexlNode node = usedLeafEntry.getValue();
                    
                    boolean isGoner = true;
                    for (Composite comp : comps) {
                        int nodeIdx = comp.jexlNodeList.indexOf(node);
                        if (nodeIdx < 0) {
                            isGoner = false;
                            break;
                        }
                    }
                    
                    if (isGoner) {
                        leafNodeMap.remove(compName, node);
                    }
                }
                
                if (comps != null && comps.size() > 0) {
                    foundCompositeMap.putAll(compositeName, comps);
                }
            }
        }
        
        return foundCompositeMap;
    }
    
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
     * Form the composite map based on child leaf nodes and available composite keys
     *
     * @param andChildNodeMap
     *            Ancestors "and" nodes
     * @param leafNodeMap
     *            Leaf nodes, {@code List<JexlNode> = bounded range; JexlNode = EqNode, ErNode, NeNode}
     * @param compositeToFieldMap
     *            Composite fields ordered from most fields to least
     * @return valid matched composite indexes
     */
    private Multimap<String,Composite> getFoundCompositeMapOr(Multimap<String,JexlNode> andChildNodeMap, Multimap<String,JexlNode> leafNodeMap,
                    Multimap<String,String> compositeToFieldMap) {
        
        Multimap<String,Composite> foundCompositeMap = ArrayListMultimap.create();
        
        // if this is an overloaded composite field, we need to also add a mapping to itself in order to ensure
        // that the ranges are expanded to include composite and non-composite terms. We add this as the last
        // composite mapping to ensure that it is only created in the event that no other composites can be formed.
        Set<Entry<String,Collection<String>>> overloadedEntries = new HashSet<>();
        for (Map.Entry<String,Collection<String>> entry : compositeToFieldMap.asMap().entrySet()) {
            if (CompositeIngest.isOverloadedCompositeField(entry.getValue(), entry.getKey()))
                overloadedEntries.add(new DefaultMapEntry(entry.getKey(), Arrays.asList(entry.getKey())));
        }
        
        for (Set<Entry<String,Collection<String>>> entries : new Set[] {compositeToFieldMap.asMap().entrySet(), overloadedEntries}) {
            // look at each potential composite name to see if its fields are all available as keys in the childNodeMap
            for (Map.Entry<String,Collection<String>> entry : entries) {
                // Is this a query against a composite field with old data whose date range predates the transition date?
                if (isQueryAgainstCompositeWithOldDataPriorToTransitionDate(entry.getKey()))
                    continue;
                
                // Did we find a larger key containing this one
                if (isCompositeFieldContainedInFoundMapWithList(foundCompositeMap, entry.getValue()))
                    continue;
                
                // Is a child node part of the key?
                if (containsAnyCompositeNodes(entry.getValue(), leafNodeMap.keySet()) == false)
                    continue;
                
                // first see if valid fields for the composite are available in the 'or' and 'and' nodes
                Set<String> leafNodeKeySet = (leafNodeMap != null) ? Sets.newHashSet(leafNodeMap.keySet()) : null;
                Set<String> andChildKeySet = (andChildNodeMap != null) ? andChildNodeMap.keySet() : null;
                List<String> compositeFields = new ArrayList<>(entry.getValue());
                for (String orLeafKey : leafNodeKeySet) {
                    if (this.containsAllCompositeNodes(entry.getValue(), Sets.newHashSet(orLeafKey), andChildKeySet)) {
                        // i can make a composite....
                        // the entry.value() collection is sorted in the correct order for the fields
                        String compositeName = entry.getKey();
                        List<Composite> comps = Lists.newArrayList();
                        Composite baseComp = new Composite(compositeName);
                        comps.add(baseComp);
                        Multimap<String,JexlNode> usedLeafNodes = HashMultimap.create();
                        for (int i = 0; i < compositeFields.size(); i++) {
                            String compField = compositeFields.get(i);
                            Collection nodes = Lists.newArrayList();
                            
                            // Track if a leaf node has been found, if not skip putting the found comps
                            if (leafNodeMap != null && compField.equalsIgnoreCase(orLeafKey)) {
                                for (JexlNode node : leafNodeMap.get(compField)) {
                                    if (isNodeValid(node, i, compositeFields.size(), config.getFixedLengthFields().contains(compField))) {
                                        nodes.add(node);
                                        usedLeafNodes.put(compField, node);
                                    }
                                }
                            }
                            
                            if (andChildNodeMap != null) {
                                for (JexlNode node : andChildNodeMap.get(compField))
                                    if (isNodeValid(node, i, compositeFields.size(), config.getFixedLengthFields().contains(compField)))
                                        nodes.add(node);
                            }
                            
                            // if at any point we run out of eligible nodes, then we have failed to build the composite, and need to stop
                            if (nodes.isEmpty()) {
                                comps = null;
                                break;
                            }
                            
                            boolean wildCardFound = updateComposites(comps, nodes);
                            if (wildCardFound) {
                                cleanInValidCompositeNodes(comps, usedLeafNodes);
                                break;
                            }
                        }
                        
                        // remove all of the leaf nodes that were successfully used
                        for (String compName : usedLeafNodes.keySet())
                            leafNodeMap.get(compName).removeAll(usedLeafNodes.get(compName));
                        
                        if (comps != null && comps.size() > 0) {
                            foundCompositeMap.putAll(compositeName, comps);
                        }
                    }
                }
            }
        }
        
        return foundCompositeMap;
    }
    
    /**
     * Search descendants for matching indexes. childrenLeafNodes are passed down as "and" nodes.
     *
     * @param andChildrenEqualsNodes
     * @param orderedCompositeToFieldMap
     * @param childrenGoners
     * @param otherNodes
     * @param descendantCompositeNodes
     * @param childrenLeafNodes
     */
    private void searchDescendants(Multimap<String,JexlNode> andChildrenEqualsNodes, LinkedHashMultimap<String,String> orderedCompositeToFieldMap,
                    Collection<JexlNode> childrenGoners, Collection<JexlNode> otherNodes, Collection<JexlNode> descendantCompositeNodes,
                    Multimap<String,JexlNode> childrenLeafNodes) {
        for (JexlNode kid : otherNodes) {
            Multimap<String,JexlNode> kidEqualsNodes = ArrayListMultimap.create();
            kidEqualsNodes.putAll(andChildrenEqualsNodes);
            if (childrenLeafNodes != null) {
                for (Entry<String,JexlNode> leafNode : childrenLeafNodes.entries()) {
                    kidEqualsNodes.put(leafNode.getKey(), leafNode.getValue());
                }
            }
            
            ExpandData kidData = new ExpandData(kid, kidEqualsNodes, orderedCompositeToFieldMap, childrenGoners);
            kid.jjtAccept(this, kidData);
            if (kidData.newNodes.size() > 0)
                descendantCompositeNodes.addAll(kidData.newNodes);
        }
        
        // Remove all goner nodes from others
        otherNodes.removeAll(childrenGoners);
        if (childrenLeafNodes != null) {
            Iterator<Entry<String,JexlNode>> childLeafNodeIter = childrenLeafNodes.entries().iterator();
            while (childLeafNodeIter.hasNext()) {
                Entry<String,JexlNode> node = childLeafNodeIter.next();
                if (childrenGoners.contains(node.getValue())) {
                    childLeafNodeIter.remove();
                }
            }
        }
    }
    
    private void cleanOtherNodes(Multimap<String,JexlNode> childrenLeafNodes, Collection<JexlNode> otherNodes) {
        for (Entry<String,JexlNode> otherEqualsNode : childrenLeafNodes.entries()) {
            otherNodes.add(otherEqualsNode.getValue());
        }
    }
    
    /**
     *
     * @param composites
     *            Composite objects being tracked for a given key
     * @param nodes
     *            Collection of nodes matching a single field
     * @return boolean if a wildcard or bounded range nodes were found.
     */
    private boolean updateComposites(List<Composite> composites, Collection<JexlNode> nodes) {
        int nodeSize = (nodes == null) ? 0 : nodes.size();
        boolean wildCardFound = false;
        
        if (nodeSize == 0) {
            return wildCardFound; // No nodes do nothing
        } else if (nodeSize == 1) {
            // Only one node cloning is not required
            JexlNode node = nodes.iterator().next();
            Iterator<Composite> compNodeListIter = composites.iterator();
            List<Composite> compRanges = new ArrayList<>();
            while (compNodeListIter.hasNext()) {
                Composite comp = compNodeListIter.next();
                
                // at this point, if this is an and node, it is a bounded range
                if (node instanceof ASTAndNode) {
                    comp = CompositeRange.clone(comp);
                    compNodeListIter.remove();
                    compRanges.add(comp);
                }
                
                addFieldToComposite(comp, node);
                if (CompositeUtils.WILDCARD_NODE_CLASSES.contains(node.getClass()) || !comp.isValid()) {
                    wildCardFound = true;
                }
            }
            composites.addAll(compRanges);
        } else { // > 1
            Iterator<JexlNode> nodesIter = nodes.iterator();
            List<Composite> cloneComps = Lists.newArrayList();
            while (nodesIter.hasNext()) {
                for (Composite comp : composites) {
                    JexlNode node = nodesIter.next();
                    Composite newComp;
                    comp.clone();
                    
                    // at this point, if this is an and node, it is a bounded range
                    if (node instanceof ASTAndNode) {
                        newComp = CompositeRange.clone(comp);
                    } else {
                        newComp = comp.clone();
                    }
                    
                    addFieldToComposite(newComp, node);
                    
                    cloneComps.add(newComp);
                    if (CompositeUtils.WILDCARD_NODE_CLASSES.contains(node.getClass()) || !newComp.isValid()) {
                        wildCardFound = true;
                    }
                    continue;
                }
            }
            composites.clear();
            composites.addAll(cloneComps);
        }
        
        return wildCardFound;
    }
    
    /**
     * Remove all composites that only have 1 or fewer nodes or first node is range or regex
     * 
     * @param composites
     *            List of found composite indexes
     */
    private void cleanInValidCompositeNodes(List<Composite> composites, Multimap<String,JexlNode> usedLeafNodes) {
        Multimap<String,JexlNode> actualUsedLeafNodes = ArrayListMultimap.create();
        Iterator<Composite> compositesIter = composites.iterator();
        while (compositesIter.hasNext()) {
            Composite comp = compositesIter.next();
            if (!comp.isValid()) {
                // when we remove a composite, we need to make sure we remove its jexl nodes from the used leaf nodes
                for (JexlNode node : comp.jexlNodeList) {
                    if (node instanceof ASTAndNode)
                        usedLeafNodes.remove(JexlASTHelper.getIdentifier(node.jjtGetChild(0)), node);
                    else
                        usedLeafNodes.remove(JexlASTHelper.getIdentifier(node), node);
                }
                compositesIter.remove();
            } else {
                // keep track of the leaf nodes that were ACTUALLY used
                for (JexlNode node : comp.jexlNodeList) {
                    if (node instanceof ASTAndNode)
                        actualUsedLeafNodes.put(JexlASTHelper.getIdentifier(node.jjtGetChild(0)), node);
                    else
                        actualUsedLeafNodes.put(JexlASTHelper.getIdentifier(node), node);
                }
            }
        }
        usedLeafNodes.clear();
        usedLeafNodes.putAll(actualUsedLeafNodes);
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
            nodes.addAll(Arrays.asList(JexlNodes.children(node)));
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
     * This function allows the addition of only fields that are included in every found composite, in case of "or" parent node Does nothing if foundComposites
     * is empty
     * 
     * @param foundComposites
     * @param andChildNodeMap
     * @param goners
     */
    private void fixOrGoners(Multimap<String,Composite> foundComposites, Multimap<String,JexlNode> andChildNodeMap, Collection<JexlNode> goners) {
        fixOrGoners(foundComposites, andChildNodeMap, goners, false);
    }
    
    private void fixOrGoners(Multimap<String,Composite> foundComposites, Multimap<String,JexlNode> andChildNodeMap, Collection<JexlNode> goners, boolean addOnly) {
        // No composites were created
        if (foundComposites.isEmpty()) {
            return;
        }
        
        for (Entry<String,JexlNode> andNodeEntry : andChildNodeMap.entries()) {
            final JexlNode andNode = andNodeEntry.getValue();
            boolean goner = isGoner(foundComposites.values(), andNode);
            
            if (goner) {
                goners.add(andNode);
            } else {
                if (!addOnly)
                    goners.remove(andNode);
            }
        }
    }
    
    private void fixAndGoners(Multimap<String,Composite> foundComposites, Multimap<String,JexlNode> andChildNodeMap, Collection<JexlNode> goners) {
        fixAndGoners(foundComposites, andChildNodeMap, goners, false);
    }
    
    /**
     * Fix and goners differs from fixOrGoners in that any match in an "and" and it can be removed.
     * 
     * @param foundComposites
     * @param andChildNodeMap
     * @param goners
     * @param addOnly
     */
    private void fixAndGoners(Multimap<String,Composite> foundComposites, Multimap<String,JexlNode> andChildNodeMap, Collection<JexlNode> goners,
                    boolean addOnly) {
        // No composites were created
        if (foundComposites.isEmpty()) {
            return;
        }
        
        for (Entry<String,JexlNode> andNodeEntry : andChildNodeMap.entries()) {
            final JexlNode andNode = andNodeEntry.getValue();
            boolean goner = isGoner(foundComposites.values(), andNode);
            
            if (goner) {
                goners.add(andNode);
            } else {
                if (!addOnly)
                    goners.remove(andNode);
            }
        }
    }
    
    private boolean isGoner(Collection<Composite> foundList, JexlNode node) {
        if (foundList == null || foundList.size() <= 0) {
            return false;
        }
        
        for (Composite foundComposite : foundList)
            if (!foundComposite.contains(node))
                return false;
        
        return true;
    }
    
    private boolean isQueryAgainstCompositeWithOldDataPriorToTransitionDate(String field) {
        if (config.getCompositeTransitionDates().containsKey(field)) {
            Date transitionDate = config.getCompositeTransitionDates().get(field);
            if (config.getEndDate().compareTo(transitionDate) < 0)
                return true;
        }
        return false;
    }
    
    /**
     *
     * @param foundCompositeMap
     * @param compositeFields
     * @return
     */
    private boolean isCompositeFieldContainedInFoundMapWithList(Multimap<String,Composite> foundCompositeMap, Collection<String> compositeFields) {
        for (Map.Entry<String,Composite> foundEntry : foundCompositeMap.entries()) {
            if (foundEntry.getValue().fieldNameList.containsAll(compositeFields)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean containsAnyCompositeNodes(Collection<String> fieldsInComposite, Set<String> fieldsInNodes) {
        for (String field : fieldsInComposite) {
            if (fieldsInNodes.contains(field)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Does the union of field sets contains all fieldsInComposite
     * 
     * @param fieldsInComposite
     *            collection of field names that are required
     * @param fieldSets
     *            find field names in these sets
     * @return join(fieldsInOrNodes, fieldsInAndNodes).containsAll(fieldsInComposite)
     */
    private boolean containsAllCompositeNodes(Collection<String> fieldsInComposite, Set<String>... fieldSets) {
        // make a copy of the fieldsInComposite so I can test by removing fields
        Set<String> tempFieldsInComposite = Sets.newHashSet(fieldsInComposite);
        for (Set<String> fieldSet : fieldSets) {
            if (fieldSet != null) {
                tempFieldsInComposite.removeAll(fieldSet);
            }
        }
        
        // If removed from both sets and used all the composite fields
        if (tempFieldsInComposite.isEmpty()) {
            return true;
        }
        return false;
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
        Collections.sort(orderedCompositeToFieldMap, new Comparator<Entry<K,Collection<V>>>() {
            @Override
            public int compare(Entry<K,Collection<V>> o1, Entry<K,Collection<V>> o2) {
                return Integer.compare(o2.getValue().size(), o1.getValue().size());
            }
        });
        
        LinkedHashMultimap<K,V> orderedMm = LinkedHashMultimap.create();
        for (Map.Entry<K,Collection<V>> foundEntry : orderedCompositeToFieldMap) {
            orderedMm.putAll(foundEntry.getKey(), foundEntry.getValue());
        }
        return orderedMm;
    }
    
    private Multimap<String,JexlNode> getChildLeafNodes(JexlNode child, Collection<JexlNode> otherNodes) {
        Multimap<String,JexlNode> childrenLeafNodes = ArrayListMultimap.create();
        
        if (child instanceof ASTAndNode) {
            // check to see if this node is a range node, if so, this is our leaf node
            JexlNode leafKid = getChildLeafNode(child);
            if (leafKid != null) {
                String kidFieldName;
                if (leafKid instanceof ASTAndNode) {
                    kidFieldName = JexlASTHelper.getIdentifier(leafKid.jjtGetChild(0));
                } else {
                    kidFieldName = JexlASTHelper.getIdentifier(leafKid);
                }
                childrenLeafNodes.put(kidFieldName, leafKid);
            }
        }
        
        if (childrenLeafNodes.isEmpty()) {
            for (JexlNode kid : JexlNodes.children(child)) {
                JexlNode leafKid = getChildLeafNode(kid);
                if (leafKid != null) {
                    String kidFieldName;
                    if (leafKid instanceof ASTAndNode) {
                        kidFieldName = JexlASTHelper.getIdentifier(leafKid.jjtGetChild(0));
                    } else {
                        kidFieldName = JexlASTHelper.getIdentifier(leafKid);
                    }
                    childrenLeafNodes.put(kidFieldName, leafKid);
                } else {
                    otherNodes.add(kid);
                }
            }
        }
        return childrenLeafNodes;
    }
    
    /**
     * Find the only child leaf node or null
     * 
     * @param node
     *            JexlNode to descend
     * @return direct and only descendant leaf node
     */
    private JexlNode getChildLeafNode(JexlNode node) {
        if (node instanceof ASTReference) {
            return getChildLeafNode((ASTReference) node);
        }
        
        if (node instanceof ASTReferenceExpression) {
            return getChildLeafNode((ASTReferenceExpression) node);
        }
        
        if (node instanceof ASTAndNode) {
            return getChildLeafNode((ASTAndNode) node);
        }
        
        if (CompositeUtils.LEAF_NODE_CLASSES.contains(node.getClass())) {
            return node;
        }
        
        return null;
    }
    
    /**
     * Find the only child leaf node or null
     * 
     * @param node
     *            JexlNode to descend
     * @return direct and only descendant leaf node
     */
    private JexlNode getChildLeafNode(ASTReference node) {
        if (node.jjtGetNumChildren() == 1) {
            JexlNode kid = node.jjtGetChild(0);
            if (kid instanceof ASTReferenceExpression) {
                return getChildLeafNode((ASTReferenceExpression) kid);
            }
        }
        return null;
    }
    
    /**
     * Find the only child leaf node or null
     * 
     * @param node
     *            JexlNode to descend
     * @return direct and only descendant leaf node
     */
    private JexlNode getChildLeafNode(ASTReferenceExpression node) {
        if (node instanceof ASTReferenceExpression && node.jjtGetNumChildren() == 1) {
            JexlNode kid = node.jjtGetChild(0);
            if (CompositeUtils.LEAF_NODE_CLASSES.contains(kid.getClass())) {
                return kid;
            } else if (kid instanceof ASTAndNode) {
                return getChildLeafNode((ASTAndNode) kid);
            } else {
                return getChildLeafNode(kid);
            }
        }
        
        return null;
    }
    
    /**
     * Find the only child leaf node or null
     *
     * @param node
     *            JexlNode to descend
     * @return direct and only descendant leaf node
     */
    private JexlNode getChildLeafNode(ASTAndNode node) {
        if (node.jjtGetNumChildren() == 1) {
            return getChildLeafNode(node.jjtGetChild(0));
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
        return null;
    }
    
    /**
     * Find the first direct descendant AND node
     * 
     * @param node
     *            JexlNode to descend
     * @return direct descendant AND node
     */
    private ASTAndNode getChildAndNode(JexlNode node) {
        if (node instanceof ASTReference) {
            return getChildAndNode((ASTReference) node);
        }
        
        if (node instanceof ASTReferenceExpression) {
            return getChildAndNode((ASTReferenceExpression) node);
        }
        return null;
    }
    
    /**
     * Find the first direct descendant AND node
     * 
     * @param node
     *            JexlNode to descend
     * @return direct descendant AND node
     */
    private ASTAndNode getChildAndNode(ASTReference node) {
        if (node.jjtGetNumChildren() == 1) {
            JexlNode kid = node.jjtGetChild(0);
            if (kid instanceof ASTReferenceExpression) {
                return getChildAndNode((ASTReferenceExpression) kid);
            }
        }
        return null;
    }
    
    /**
     * Find the first direct descendant AND node
     * 
     * @param node
     *            JexlNode to descend
     * @return direct descendant AND node
     */
    private ASTAndNode getChildAndNode(ASTReferenceExpression node) {
        if (node instanceof ASTReferenceExpression && node.jjtGetNumChildren() == 1) {
            JexlNode kid = node.jjtGetChild(0);
            if (kid instanceof ASTAndNode) {
                return (ASTAndNode) kid;
            } else {
                return getChildAndNode(kid);
            }
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
    
}
