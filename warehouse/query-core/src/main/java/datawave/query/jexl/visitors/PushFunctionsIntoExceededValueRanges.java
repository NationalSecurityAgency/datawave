package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static org.apache.commons.jexl3.parser.JexlNodes.newInstanceOfType;
import static org.apache.commons.jexl3.parser.JexlNodes.setChildren;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

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
     *            a script
     * @param helper
     *            the metadata helper
     * @param datatypeFilter
     *            the datatype filter
     * @param <T>
     *            type of the script
     * @return the modified node tree
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T pushFunctions(T script, MetadataHelper helper, Set<String> datatypeFilter) {
        PushFunctionsIntoExceededValueRanges visitor = new PushFunctionsIntoExceededValueRanges(helper, datatypeFilter);

        return (T) (script.jjtAccept(visitor, null));
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // first recurse and create the copy
        node = (ASTAndNode) (super.visit(node, data));

        // if this and node itself is the exceeded value threshold, then abort
        if (QueryPropertyMarker.findInstance(node).isType(EXCEEDED_VALUE)) {
            return node;
        }

        // find all of the single field function nodes, and all of the exceeded value threshold range nodes and map by field name
        // place all other nodes into the children list
        Set<JexlNode> functionNodes = new HashSet<>();
        Multimap<String,JexlNode> functionNodesByField = HashMultimap.create();
        Multimap<String,JexlNode> exceededValueRangeNodes = HashMultimap.create();
        List<JexlNode> children = new ArrayList<>();
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
            JexlNodes.copyImage(node, newNode);
            for (String field : fields) {
                boolean copyFunction = exceededValueRangeNodes.get(field).size() > 1;
                for (JexlNode range : exceededValueRangeNodes.removeAll(field)) {

                    // filter out functions which disallow ivarator filtering
                    Collection<JexlNode> filterableFunctions = functionNodesByField.get(field).stream().filter(functionNode -> {
                        JexlArgumentDescriptor argDesc = JexlFunctionArgumentDescriptorFactory.F
                                        .getArgumentDescriptor((ASTFunctionNode) JexlASTHelper.dereference(functionNode));
                        return argDesc != null && argDesc.allowIvaratorFiltering();
                    }).collect(Collectors.toList());
                    functionNodes.removeAll(filterableFunctions);
                    if (copyFunction) {
                        filterableFunctions = new HashSet<>();
                        for (JexlNode function : functionNodesByField.get(field)) {
                            filterableFunctions.add(super.copy(function));
                        }
                    }
                    children.add(pushFunctionIntoExceededValueRange(filterableFunctions, range));
                    functionNodes.removeAll(filterableFunctions);
                }
            }

            // now add in the children that remain
            children.addAll(functionNodes);
            children.addAll(exceededValueRangeNodes.values());

            // and return the new and node with the new children
            return setChildren(newNode, children.toArray(new JexlNode[children.size()]));
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
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(child);
        if (instance.isType(EXCEEDED_VALUE)) {
            JexlNode source = instance.getSource();
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
        JexlNode source = QueryPropertyMarker.findInstance(child).getSource();
        source = JexlASTHelper.dereference(source);
        List<ASTIdentifier> fields = JexlASTHelper.getIdentifiers(source);
        return fields.get(0).getName();
    }

    private JexlNode pushFunctionIntoExceededValueRange(Collection<JexlNode> functions, JexlNode range) {
        JexlNode source = QueryPropertyMarker.findInstance(range).getSource();
        source = JexlASTHelper.dereference(source);
        JexlNode parent = source.jjtGetParent();
        ASTAndNode andNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        JexlNode[] allChildren = new JexlNode[functions.size() + 1];
        int i = 0;
        allChildren[i++] = source;
        for (JexlNode function : functions) {
            allChildren[i++] = function;
        }
        andNode = setChildren(andNode, allChildren);
        setChildren(parent, andNode);
        return range;
    }

}
