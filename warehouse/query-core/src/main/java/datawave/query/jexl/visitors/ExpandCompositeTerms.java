package datawave.query.jexl.visitors;

import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.*;
import datawave.data.type.NoOpType;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.util.Composite;
import datawave.query.util.CompositeNameAndIndex;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.google.common.base.Preconditions;

/**
 *
 */
public class ExpandCompositeTerms extends RebuildingVisitor {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(ExpandCompositeTerms.class);
    
    private final ShardQueryConfiguration config;
    protected MetadataHelper helper;
    
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
    
    private ExpandCompositeTerms(ShardQueryConfiguration config, MetadataHelper helper) {
        Preconditions.checkNotNull(config);
        this.config = config;
        this.helper = helper;
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
        
        ExpandCompositeTerms visitor = new ExpandCompositeTerms(config, helper);
        
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
        
        Multimap<String,CompositeNameAndIndex> fieldToCompositeMap = config.getFieldToCompositeMap();
        Collection<JexlNode> andChildrenGoners = Sets.newHashSet();
        boolean hasEqNode = false;
        for (JexlNode kid : JexlNodes.children(node)) {
            if (Composite.LEAF_NODE_CLASSES.contains(kid.getClass()) && fieldToCompositeMap.containsKey(JexlASTHelper.getIdentifier(kid))) {
                hasEqNode = true;
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
            JexlNode orNode = JexlNodeFactory.createOrNode(nodeList);
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
            JexlNode andNode = JexlNodeFactory.createAndNode(nodeList);
            
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
            int last = comp.jexlNodeList.size() - 1;
            JexlNode compositeNode = comp.jexlNodeList.get(last);
            
            JexlNode newNode = null;
            if (compositeNode instanceof ASTGTNode) {
                newNode = JexlNodeFactory.buildNode((ASTGTNode) null, comp.compositeName, comp.getAppendedExpressions());
            } else if (compositeNode instanceof ASTGENode) {
                newNode = JexlNodeFactory.buildNode((ASTGENode) null, comp.compositeName, comp.getAppendedExpressions());
            } else if (compositeNode instanceof ASTLTNode) {
                newNode = JexlNodeFactory.buildNode((ASTLTNode) null, comp.compositeName, comp.getAppendedExpressions());
            } else if (compositeNode instanceof ASTLENode) {
                newNode = JexlNodeFactory.buildNode((ASTLENode) null, comp.compositeName, comp.getAppendedExpressions());
            } else if (compositeNode instanceof ASTERNode) {
                newNode = JexlNodeFactory.buildERNode(comp.compositeName, comp.getAppendedExpressions());
            } else if (compositeNode instanceof ASTNENode) {
                newNode = JexlNodeFactory.buildNode((ASTNENode) null, comp.compositeName, comp.getAppendedExpressions());
            } else if (compositeNode instanceof ASTEQNode) {
                // This is an equals node put it in the nodeList
                newNode = JexlNodeFactory.buildEQNode(comp.compositeName, comp.getAppendedExpressions());
            } else {
                log.error("Invalid or unknown node type for composite map.");
            }
            
            if (newNode != null) {
                nodeList.add(newNode);
            }
            
            config.getIndexedFields().add(comp.compositeName);
            config.getQueryFieldsDatatypes().put(comp.compositeName, new NoOpType());
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
        // All child eq and bounded range nodes if part of composite or not
        // ex. [{id1=>[node1, boundedNode2]},{key1=>[node3, node4]},{key2=>[node5,node6]}]
        // if Object is a List it is a bounded range otherwise JexlNode
        Multimap<String,JexlNode> childrenLeafNodes = getChildLeafNodes(child, otherNodes);
        searchDescendants(andChildrenEqualsNodes, orderedCompositeToFieldMap, childrenGoners, otherNodes, descendantCompositeNodes, childrenLeafNodes);
        
        Multimap<String,Composite> leafFound = getFoundCompositeMapAnd(andChildrenEqualsNodes, childrenLeafNodes, orderedCompositeToFieldMap, childrenGoners);
        fixAndGoners(leafFound, andChildrenEqualsNodes, childrenGoners);
        
        cleanOtherNodes(childrenLeafNodes, otherNodes);
        
        return leafFound;
    }
    
    private Multimap<String,Composite> getFoundCompositeMap(ASTOrNode child, Multimap<String,JexlNode> andChildrenEqualsNodes,
                    LinkedHashMultimap<String,String> orderedCompositeToFieldMap, Collection<JexlNode> childrenGoners, Collection<JexlNode> otherNodes,
                    Collection<JexlNode> descendantCompositeNodes) {
        
        // All child eq and bounded range nodes if part of composite or not
        // ex. [{id1=>[node1, boundedNode2]},{key1=>[node3, node4]},{key2=>[node5,node6]}]
        // if Object is a List it is a bounded range otherwise JexlNode
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
    private Multimap<String,Composite> getFoundCompositeMapAnd(Multimap<String,JexlNode> andChildNodeMap, Multimap<String,? extends Object> leafNodeMap,
                    Multimap<String,String> compositeToFieldMap, Collection<JexlNode> goners) {
        
        Multimap<String,Composite> foundCompositeMap = ArrayListMultimap.create();
        
        // look at each potential composite name to see if its fields are all available as keys in the childNodeMap
        for (Map.Entry<String,Collection<String>> entry : compositeToFieldMap.asMap().entrySet()) {
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
            for (String field : entry.getValue()) {
                Collection nodes = Lists.newArrayList();
                
                if (leafNodeMap != null) {
                    nodes.addAll(leafNodeMap.get(field));
                }
                
                if (andChildNodeMap != null) {
                    nodes.addAll(andChildNodeMap.get(field));
                }
                
                boolean wildCardFound = updateComposites(comps, nodes);
                leafNodeMap.removeAll(field);
                if (wildCardFound) {
                    // Wild card can introduce single value comp keys and comps with out leaf nodes
                    cleanInValidCompositeNodes(comps, leafNodeKeySet);
                    break;
                }
            }
            
            if (comps != null && comps.size() > 0) {
                foundCompositeMap.putAll(compositeName, comps);
            }
        }
        
        return foundCompositeMap;
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
    private Multimap<String,Composite> getFoundCompositeMapOr(Multimap<String,JexlNode> andChildNodeMap, Multimap<String,? extends Object> leafNodeMap,
                    Multimap<String,String> compositeToFieldMap) {
        
        Multimap<String,Composite> foundCompositeMap = ArrayListMultimap.create();
        
        // look at each potential composite name to see if its fields are all available as keys in the childNodeMap
        for (Map.Entry<String,Collection<String>> entry : compositeToFieldMap.asMap().entrySet()) {
            // Did we find a larger key containing this one
            if (isCompositeFieldContainedInFoundMapWithList(foundCompositeMap, entry.getValue()))
                continue;
            
            // Is a child node part of the key?
            if (containsAnyCompositeNodes(entry.getValue(), leafNodeMap.keySet()) == false)
                continue;
            
            // first see if valid fields for the composite are available in the 'or' and 'and' nodes
            Set<String> leafNodeKeySet = (leafNodeMap != null) ? leafNodeMap.keySet() : null;
            Set<String> andChildKeySet = (andChildNodeMap != null) ? andChildNodeMap.keySet() : null;
            Set<String> leafNodeGoners = Sets.newHashSet();
            for (String orLeafKey : leafNodeKeySet) {
                if (this.containsAllCompositeNodes(entry.getValue(), Sets.newHashSet(orLeafKey), andChildKeySet)) {
                    // i can make a composite....
                    // the entry.value() collection is sorted in the correct order for the fields
                    String compositeName = entry.getKey();
                    List<Composite> comps = Lists.newArrayList();
                    Composite baseComp = new Composite(compositeName);
                    comps.add(baseComp);
                    for (String field : entry.getValue()) {
                        Collection nodes = Lists.newArrayList();
                        
                        // Track if a leaf node has been found it not skip putting the found comps
                        if (leafNodeMap != null && field.equalsIgnoreCase(orLeafKey)) {
                            nodes.addAll(leafNodeMap.get(field));
                            leafNodeGoners.add(field);
                        }
                        
                        if (andChildNodeMap != null) {
                            nodes.addAll(andChildNodeMap.get(field));
                        }
                        
                        boolean wildCardFound = updateComposites(comps, nodes);
                        if (wildCardFound) {
                            cleanInValidCompositeNodes(comps, Sets.newHashSet(orLeafKey));
                            break;
                        }
                    }
                    
                    if (comps != null && comps.size() > 0) {
                        foundCompositeMap.putAll(compositeName, comps);
                    }
                }
            }
            
            for (String leafNodeGoner : leafNodeGoners) {
                leafNodeMap.removeAll(leafNodeGoner);
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
                    // Only pass down eq nodes not bounded ranges
                    if (leafNode.getValue() instanceof JexlNode) {
                        kidEqualsNodes.put(leafNode.getKey(), leafNode.getValue());
                    }
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
            while (compNodeListIter.hasNext()) {
                Composite comp = compNodeListIter.next();
                addFieldToComposite(comp, node);
                if (Composite.WILDCARD_NODE_CLASSES.contains(node.getClass())) {
                    wildCardFound = true;
                }
            }
        } else { // > 1
            Iterator<JexlNode> nodesIter = nodes.iterator();
            List<Composite> cloneComps = Lists.newArrayList();
            while (nodesIter.hasNext()) {
                for (Composite comp : composites) {
                    JexlNode node = nodesIter.next();
                    Composite newComp = comp.clone();
                    addFieldToComposite(newComp, node);
                    
                    cloneComps.add(newComp);
                    if (Composite.WILDCARD_NODE_CLASSES.contains(node.getClass())) {
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
    private void cleanInValidCompositeNodes(List<Composite> composites, Set<String> leafKeySet) {
        Iterator<Composite> compositesIter = composites.iterator();
        while (compositesIter.hasNext()) {
            Composite comp = compositesIter.next();
            boolean isValid = comp.isValid();
            if (isValid) {
                for (String leafKey : leafKeySet) {
                    if (comp.fieldNameList.contains(leafKey)) {
                        isValid = true;
                        break;
                    }
                    isValid = false;
                }
            }
            
            if (!isValid) {
                compositesIter.remove();
            }
        }
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
        Object lit = JexlASTHelper.getLiteralValue(node);
        String identifier = JexlASTHelper.getIdentifier(node);
        composite.jexlNodeList.add(node);
        composite.fieldNameList.add(identifier);
        composite.expressionList.add(lit.toString());
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
        
        for (Composite foundComposite : foundList) {
            if (!foundComposite.contains(node)) {
                return false;
            }
        }
        return true;
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
     * Does the union of field sets contains more then one field in order
     *
     * @param fieldsInComposite
     *            Field names to locate
     * @param fieldSets
     *            Sets of field names
     * @return join(fieldSets...).containsAll(fieldsInComposite)
     */
    private boolean containsPartCompositeNodes(Collection<String> fieldsInComposite, Set<String>... fieldSets) {
        int nodes = 0;
        for (String fieldInComposite : fieldsInComposite) {
            boolean found = false;
            for (Set<String> fieldSet : fieldSets) {
                if (fieldSet != null && fieldSet.contains(fieldInComposite)) {
                    nodes++;
                    found = true;
                    break;
                }
            }
            if (!found) {
                break;
            }
        }
        
        if (nodes >= 2) {
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
    
    /**
     * Get a JexlNode from andChildNodeMap with fieldName
     * 
     * @param fieldName
     * @param childNodeMap
     * @return return the first JexlNode in {@code map->fieldName}
     */
    private JexlNode getNodeFromAndMap(String fieldName, Multimap<String,JexlNode> childNodeMap) {
        JexlNode foundNode = null;
        Collection<JexlNode> childJexlNodes = childNodeMap.get(fieldName);
        
        if (childJexlNodes.isEmpty() == false) {
            foundNode = childJexlNodes.iterator().next();
        }
        
        return foundNode;
    }
    
    private Multimap<String,JexlNode> getChildLeafNodes(JexlNode child, Collection<JexlNode> otherNodes) {
        Multimap<String,JexlNode> childrenLeafNodes = ArrayListMultimap.create();
        
        for (JexlNode kid : JexlNodes.children(child)) {
            JexlNode leafKid = getChildLeafNode(kid);
            if (leafKid != null) {
                String kidFieldName = JexlASTHelper.getIdentifier(leafKid);
                childrenLeafNodes.put(kidFieldName, leafKid);
            } else {
                otherNodes.add(kid);
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
        
        if (Composite.LEAF_NODE_CLASSES.contains(node.getClass())) {
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
            if (Composite.LEAF_NODE_CLASSES.contains(kid.getClass())) {
                return kid;
            } else {
                return getChildLeafNode(kid);
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
