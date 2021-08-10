package datawave.query.jexl.visitors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.attributes.UniqueFields;
import datawave.query.attributes.UniqueGranularity;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Visits the query tree and if there is a f:options function, extracts the parameters into a Map
 *
 */
public class QueryOptionsFromQueryVisitor extends RebuildingVisitor {
    
    private static final Set<String> RESERVED = ImmutableSet.of(QueryFunctions.QUERY_FUNCTION_NAMESPACE, QueryFunctions.OPTIONS_FUNCTION,
                    QueryFunctions.UNIQUE_FUNCTION, QueryFunctions.UNIQUE_BY_DAY_FUNCTION, QueryFunctions.UNIQUE_BY_HOUR_FUNCTION,
                    QueryFunctions.UNIQUE_BY_MINUTE_FUNCTION, QueryFunctions.GROUPBY_FUNCTION);
    
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
        } else if (data instanceof UniqueFields) {
            return this.visit(node, data);
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
            switch (node.jjtGetChild(1).image) {
                case QueryFunctions.OPTIONS_FUNCTION: {
                    List<String> optionsList = new ArrayList<>();
                    this.visit(node, optionsList);
                    // Parse the options List pairs into the map as key,value,key,value....
                    for (int i = 0; i + 1 < optionsList.size(); i++) {
                        String key = optionsList.get(i++);
                        String value = optionsList.get(i);
                        optionsMap.put(key, value);
                    }
                    return null;
                }
                case QueryFunctions.UNIQUE_FUNCTION: {
                    // Get the list of declared fields and join them into a comma-delimited string.
                    List<String> fieldList = new ArrayList<>();
                    this.visit(node, fieldList);
                    String fieldString = String.join(Constants.COMMA, fieldList);
                    
                    // Parse the unique fields.
                    UniqueFields uniqueFields = UniqueFields.from(fieldString);
                    updateUniqueFieldsOption(optionsMap, uniqueFields);
                    return null;
                }
                case QueryFunctions.UNIQUE_BY_DAY_FUNCTION: {
                    UniqueFields uniqueFields = new UniqueFields();
                    putFieldsFromChildren(node, uniqueFields, UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY);
                    updateUniqueFieldsOption(optionsMap, uniqueFields);
                    return null;
                }
                case QueryFunctions.UNIQUE_BY_HOUR_FUNCTION: {
                    UniqueFields uniqueFields = new UniqueFields();
                    putFieldsFromChildren(node, uniqueFields, UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
                    updateUniqueFieldsOption(optionsMap, uniqueFields);
                    return null;
                }
                case QueryFunctions.UNIQUE_BY_MINUTE_FUNCTION: {
                    UniqueFields uniqueFields = new UniqueFields();
                    putFieldsFromChildren(node, uniqueFields, UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE);
                    updateUniqueFieldsOption(optionsMap, uniqueFields);
                    return null;
                }
                case QueryFunctions.GROUPBY_FUNCTION: {
                    List<String> optionsList = new ArrayList<>();
                    this.visit(node, optionsList);
                    optionsMap.put(QueryParameters.GROUP_FIELDS, Joiner.on(',').join(optionsList));
                    return null;
                }
            }
        }
        return super.visit(node, optionsMap);
    }
    
    /**
     * Find all unique fields declared in the provided node and add them to the provided {@link UniqueFields} with the specified transformer.
     * 
     * @param node
     *            the node to find fields in
     * @param uniqueFields
     *            the unique fields instance to modify
     * @param transformer
     *            the transformer to add any identified fields with.
     */
    private void putFieldsFromChildren(JexlNode node, UniqueFields uniqueFields, UniqueGranularity transformer) {
        List<String> fields = new ArrayList<>();
        this.visit(node, fields);
        fields.forEach((field) -> uniqueFields.put(field, transformer));
    }
    
    /**
     * Update the {@value QueryParameters#UNIQUE_FIELDS} option to include the given unique fields.
     * 
     * @param optionsMap
     *            the options map
     * @param uniqueFields
     *            the unique fields to include
     */
    private void updateUniqueFieldsOption(Map<String,String> optionsMap, UniqueFields uniqueFields) {
        // Combine with any previously found unique fields.
        if (optionsMap.containsKey(QueryParameters.UNIQUE_FIELDS)) {
            UniqueFields existingFields = UniqueFields.from(optionsMap.get(QueryParameters.UNIQUE_FIELDS));
            uniqueFields.putAll(existingFields.getFieldMap());
        }
        optionsMap.put(QueryParameters.UNIQUE_FIELDS, uniqueFields.toString());
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
     * if the passed data is a List, call the method that collects the node image strings
     *
     * @param node
     *            an identifier
     * @param data
     *            userData
     * @return the rebuilt node
     */
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        if (!RESERVED.contains(node.image)) {
            if (data instanceof List) {
                return this.visit(node, (List) data);
            }
        }
        return super.visit(node, data);
    }
    
    /**
     * collect the node.image strings into the passed List
     *
     * @param node
     *            the ASTIdentifier that is a child of the f:options function
     * @param list
     *            a list for collecting the child image strings (the property key/values)
     * @return
     */
    private Object visit(ASTIdentifier node, List<String> list) {
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
