package datawave.query.jexl.visitors;

import com.google.common.base.Joiner;
import datawave.query.QueryParameters;
import datawave.query.jexl.functions.QueryFunctions;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Visits the query tree and if there is a f:options function, extracts the parameters into a Map
 *
 */
public class QueryOptionsFromQueryVisitor extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(QueryOptionsFromQueryVisitor.class);
    
    private List<String> optionsList = new ArrayList<>();
    
    /**
     * If the passed userData is a Map, type cast and call the method to begin collection of the function arguments
     * 
     * @param node
     *            potentially a f:options function node
     * @param data
     *            userData
     * @return the rebuilt node
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        if (data instanceof Map) {
            return this.visit(node, (Map) data);
        }
        return super.visit(node, data);
    }
    
    /**
     * If this is the f:options function, descend the tree with a List in the passed userdata. The function args will be collected into the List when visiting
     * the child ASTStringLiteral nodes
     * 
     * @param node
     *            the function node potentially for f:options
     * @param optionsMap
     *            a Map to return option key/values
     * @return the rebuilt node
     */
    private Object visit(ASTFunctionNode node, Map<String,String> optionsMap) {
        // if this is the f:options function, create a List for the userData to be passed to the child nodes
        if (node.jjtGetChild(0).image.equals(QueryFunctions.QUERY_FUNCTION_NAMESPACE)) {
            if (node.jjtGetChild(1).image.equals(QueryFunctions.OPTIONS_FUNCTION)) {
                List<String> optionsList = new ArrayList<>();
                Object ret = this.visit(node, optionsList);
                // Parse the options List pairs into the map as key,value,key,value....
                for (int i = 0; i + 1 < optionsList.size(); i++) {
                    String key = optionsList.get(i++);
                    String value = optionsList.get(i);
                    optionsMap.put(key, value);
                }
                return null;
            } else if (node.jjtGetChild(1).image.equals(QueryFunctions.UNIQUE_FUNCTION)) {
                List<String> optionsList = new ArrayList<>();
                Object ret = this.visit(node, optionsList);
                optionsMap.put(QueryParameters.UNIQUE_FIELDS, Joiner.on(',').join(optionsList));
                return null;
            } else if (node.jjtGetChild(1).image.equals(QueryFunctions.GROUPBY_FUNCTION)) {
                List<String> optionsList = new ArrayList<>();
                Object ret = this.visit(node, optionsList);
                optionsMap.put(QueryParameters.GROUP_FIELDS, Joiner.on(',').join(optionsList));
                return null;
            }
        }
        return super.visit(node, optionsMap);
    }
    
    /**
     * if the passed data is a List, call the method that collects the node image strings
     * 
     * @param node
     *            a string literal
     * @param data
     *            userData
     * @return the rebuilt node
     */
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        if (data instanceof List) {
            return this.visit(node, (List) data);
        }
        return super.visit(node, data);
    }
    
    /**
     * collect the node.image strings into the passed List
     * 
     * @param node
     *            the ASTLiteralNode that is a child of the f:options function
     * @param list
     *            a list for collecting the child image strings (the property key/values)
     * @return
     */
    private Object visit(ASTStringLiteral node, List<String> list) {
        list.add(node.image);
        return super.visit(node, list);
    }
    
    /**
     * If the visit to the ASTFunction node returned null (because it was the options function) then there could be an empty ASTReference node. This would
     * generate a parseException in the jexl Parser unless the empty ASTReference node is also removed by returning null here.
     * 
     * @param node
     * @param data
     * @return
     */
    @Override
    public Object visit(ASTReference node, Object data) {
        JexlNode n = (JexlNode) super.visit(node, data);
        if (n.jjtGetNumChildren() == 0)
            return null;
        return n;
    }
    
    /**
     * If the visit to the ASTFunction node returned null (because it was the options function) then there could be an empty ASTReferenceExpression node. This
     * would generate a parseException in the jexl Parser unless the empty ASTReferenceExpression node is also removed by returning null here.
     *
     * @param node
     * @param data
     * @return
     */
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        JexlNode n = (JexlNode) super.visit(node, data);
        if (n.jjtGetNumChildren() == 0)
            return null;
        return n;
        
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T collect(T node, Object data) {
        QueryOptionsFromQueryVisitor visitor = new QueryOptionsFromQueryVisitor();
        return (T) node.jjtAccept(visitor, data);
    }
}
