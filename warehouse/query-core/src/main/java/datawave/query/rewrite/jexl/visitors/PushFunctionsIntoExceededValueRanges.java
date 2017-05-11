package datawave.query.rewrite.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.query.rewrite.jexl.JexlASTHelper;
import datawave.query.rewrite.jexl.JexlNodeFactory;
import datawave.query.rewrite.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.*;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.newInstanceOfType;

/**
 * Visits an JexlNode tree, pushing functions into exceeded value ranges. This is to enable use of the filtering ivarator instead of simply the range ivarator.
 * The idea is to enable handling ranges for which there are many false positives relative to the matching function. The fields for the functions must match
 * that of the range. Presumably the range was created by the getIndexQuery on the function descriptor.
 *
 */
public class PushFunctionsIntoExceededValueRanges extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(PushFunctionsIntoExceededValueRanges.class);
    
    private MetadataHelper helper;
    private Set<String> datatypeFilter;
    
    public PushFunctionsIntoExceededValueRanges(MetadataHelper helper, Set<String> datatypeFilter) {
        this.helper = helper;
        this.datatypeFilter = datatypeFilter;
    }
    
    /**
     * push functions into exceeded value threshold ranges.
     *
     * @param script
     * @return the modified node tree
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T pushFunctions(T script, MetadataHelper helper, Set<String> datatypeFilter) {
        PushFunctionsIntoExceededValueRanges visitor = new PushFunctionsIntoExceededValueRanges(helper, datatypeFilter);
        
        T node = (T) (script.jjtAccept(visitor, null));
        
        return node;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // first recurse and create the copy
        node = (ASTAndNode) (super.visit(node, data));
        
        // if this and node itself is the exceeded value threshold, then abort
        if (ExceededValueThresholdMarkerJexlNode.instanceOf(node)) {
            return node;
        }
        
        // find all of the single field function nodes, and all of the exceeded value threshold range nodes and map by field name
        // place all other nodes into the children list
        Set<JexlNode> functionNodes = new HashSet<>();
        Multimap<String,JexlNode> functionNodesByField = HashMultimap.create();
        Multimap<String,JexlNode> exceededValueRangeNodes = HashMultimap.create();
        List<JexlNode> children = new ArrayList<JexlNode>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            if (isSingleFieldFunctionNode(child)) {
                functionNodes.add(child);
                for (String field : getFunctionFields(child)) {
                    functionNodesByField.put(field, child);
                }
            } else if (isExceededValueRangeNode(child)) {
                exceededValueRangeNodes.put(getRangeField(child), child);
            } else {
                children.add(child);
            }
        }
        
        // determine if we have any functions that can be combined with the execeeded value threshold ranges
        Set<String> fields = Sets.intersection(functionNodesByField.keySet(), exceededValueRangeNodes.keySet());
        if (fields.isEmpty()) {
            return node;
        } else {
            // we have a cross section, so for each of those fields, push the functions into the exceeded value threshold along side of the range
            ASTAndNode newNode = newInstanceOfType(node);
            newNode.image = node.image;
            for (String field : fields) {
                boolean copyFunction = exceededValueRangeNodes.get(field).size() > 1;
                for (JexlNode range : exceededValueRangeNodes.removeAll(field)) {
                    Collection<JexlNode> functions = functionNodesByField.get(field);
                    functionNodes.removeAll(functions);
                    if (copyFunction) {
                        functions = new HashSet<>();
                        for (JexlNode function : functionNodesByField.get(field)) {
                            functions.add(super.copy(function));
                        }
                    }
                    children.add(pushFunctionIntoExceededValueRange(functions, range));
                    functionNodes.removeAll(functions);
                }
            }
            
            // now add in the children that remain
            children.addAll(functionNodes);
            children.addAll(exceededValueRangeNodes.values());
            
            // and return the new and node with the new children
            return children(newNode, children.toArray(new JexlNode[children.size()]));
        }
    }
    
    private boolean isSingleFieldFunctionNode(JexlNode child) {
        child = JexlASTHelper.dereference(child);
        if (child instanceof ASTFunctionNode) {
            Set<Set<String>> fieldSets = JexlASTHelper.getFieldNameSets((ASTFunctionNode) child, helper, datatypeFilter);
            if (fieldSets.isEmpty()) {
                return false;
            }
            for (Set<String> fields : fieldSets) {
                if (fields.size() != 1) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    private Set<String> getFunctionFields(JexlNode child) {
        child = JexlASTHelper.dereference(child);
        if (child instanceof ASTFunctionNode) {
            return JexlASTHelper.getFieldNames((ASTFunctionNode) child, helper, datatypeFilter);
        }
        return null;
    }
    
    private boolean isExceededValueRangeNode(JexlNode child) {
        if (ExceededValueThresholdMarkerJexlNode.instanceOf(child)) {
            JexlNode source = ExceededValueThresholdMarkerJexlNode.getExceededValueThresholdSource(child);
            source = JexlASTHelper.dereference(source);
            if (source instanceof ASTAndNode && source.jjtGetNumChildren() == 2) {
                JexlNode sourceChild = JexlASTHelper.dereference(source.jjtGetChild(0));
                if (sourceChild instanceof ASTLENode || sourceChild instanceof ASTLTNode || sourceChild instanceof ASTGENode
                                || sourceChild instanceof ASTGTNode) {
                    sourceChild = JexlASTHelper.dereference(source.jjtGetChild(1));
                    if (sourceChild instanceof ASTLENode || sourceChild instanceof ASTLTNode || sourceChild instanceof ASTGENode
                                    || sourceChild instanceof ASTGTNode) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    
    private String getRangeField(JexlNode child) {
        JexlNode source = ExceededValueThresholdMarkerJexlNode.getExceededValueThresholdSource(child);
        source = JexlASTHelper.dereference(source);
        List<ASTIdentifier> fields = JexlASTHelper.getIdentifiers(source);
        return fields.get(0).image;
    }
    
    private JexlNode pushFunctionIntoExceededValueRange(JexlNode filter, JexlNode range) {
        JexlNode source = ExceededValueThresholdMarkerJexlNode.getExceededValueThresholdSource(range);
        source = JexlASTHelper.dereference(source);
        JexlNode parent = source.jjtGetParent();
        ASTAndNode andNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        andNode = children(andNode, source, filter);
        children(parent, andNode);
        return range;
    }
    
    private JexlNode pushFunctionIntoExceededValueRange(Collection<JexlNode> functions, JexlNode range) {
        JexlNode source = ExceededValueThresholdMarkerJexlNode.getExceededValueThresholdSource(range);
        source = JexlASTHelper.dereference(source);
        JexlNode parent = source.jjtGetParent();
        ASTAndNode andNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        JexlNode[] allChildren = new JexlNode[functions.size() + 1];
        int i = 0;
        allChildren[i++] = source;
        for (JexlNode function : functions) {
            allChildren[i++] = function;
        }
        andNode = children(andNode, allChildren);
        children(parent, andNode);
        return range;
    }
    
}
