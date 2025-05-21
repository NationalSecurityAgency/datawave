package datawave.query.jexl.visitors;

import static org.apache.commons.jexl3.parser.ParserTreeConstants.JJTANDNODE;
import static org.apache.commons.jexl3.parser.ParserTreeConstants.JJTORNODE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import datawave.query.QueryParameters;
import datawave.query.attributes.UniqueFields;
import datawave.query.attributes.UniqueGranularity;
import datawave.query.jexl.functions.QueryFunctions;

/**
 * Visits the query tree and extracts the parameters from any options functions present and adds them to the provided data {@link Map}. Any options function
 * nodes will be subsequently deleted and remaining AND/OR nodes will be optimized to account for the potential removal of their children. The supported options
 * are as followed:
 * <ul>
 * <li>{@code f:options()}: Expects a comma-delimited list of key/value pairs, e.g. {@code f:options('hit.list','true','limit.fields','FOO_1_BAR=3)}</li>
 * <li>{@code f:groupby()}: Expects a comma-delimited list of fields to group by, e.g. {@code f:groupby('field1','field2','field3')}</li>
 * <li>{@code f:noexpansion()}: Expects a comma-delimited list of fields, e.g. {@code f:noExpansion('field1','field2','field3')}</li>
 * <li>{@code f:lenient()}: Expects a comma-delimited list of fields, e.g. {@code f:lenient('field1','field2','field3')}</li>
 * <li>{@code f:strict()}: Expects a comma-delimited list of fields, e.g. {@code f:strict('field1','field2','field3')}</li>
 * <li>{@code f:excerpt_fields()}: Expects a comma-delimited list of fields, e.g. {@code f:excerpt_fields('field1','field2','field3')}</li>
 * <li>{@code f:unique()}: Expects a comma-delimited list of fields to be unique and their granularity levels, e.g.
 * {@code f:unique('field1[ALL]','field2[DAY]','field3[MINUTE,SECOND]')}</li>
 * <li>{@code f:unique_by_day()}: Expects a comma-delimited list of fields to be unique with a granularity level of by DAY, e.g.
 * {@code unique_by_day('field1','field2')}</li>
 * <li>{@code f:unique_by_minute()}: Expects a comma-delimited list of fields to be unique with a granularity level of by MINUTE, e.g.
 * {@code unique_by_minute('field1','field2')}</li>
 * <li>{@code f:unique_by_second()}: Expects a comma-delimited list of fields to be unique with a granularity level of by SECOND, e.g.
 * {@code unique_by_second('field1','field2')}</li>
 * <li>{@code f:most_recent_unique...} Adding most_recent_ before any unique function will set the most.recent.unique flag to true, e.g.
 * {@code most_recent_unique_by_day('field1','field2')}</li>
 * <li>{@code f:rename}: Expects a comma-delimited list field/field mappings e.g. {@code f:rename('field1=field2','field3=field4')}</li>
 * <li>{@code f:sum}: Expects a comma-delimited list of fields to sum the values of in a grouping.</li>
 * <li>{@code f:min}: Expects a comma-delimited list of fields to find the minimum value of in a grouping.</li>
 * <li>{@code f:max}: Expects a comma-delimited list of fields to find the maximum value of in a grouping.</li>
 * <li>{@code f:average}: Expects a comma-delimited list of fields to find the average value of in a grouping.</li>
 * <li>{@code f:count}: Expects a comma-delimited list of fields to count the occurrences of in a grouping.</li>
 * </ul>
 */
public class QueryOptionsFromQueryVisitor extends RebuildingVisitor {

    private static final Joiner JOINER = Joiner.on(',').skipNulls();

    private static final Set<String> RESERVED = ImmutableSet.of(QueryFunctions.QUERY_FUNCTION_NAMESPACE, QueryFunctions.OPTIONS_FUNCTION,
                    QueryFunctions.UNIQUE_FUNCTION, UniqueFunction.UNIQUE_BY_DAY_FUNCTION, UniqueFunction.UNIQUE_BY_HOUR_FUNCTION,
                    UniqueFunction.UNIQUE_BY_MINUTE_FUNCTION, UniqueFunction.UNIQUE_BY_TENTH_OF_HOUR_FUNCTION, UniqueFunction.UNIQUE_BY_MONTH_FUNCTION,
                    UniqueFunction.UNIQUE_BY_SECOND_FUNCTION, UniqueFunction.UNIQUE_BY_MILLISECOND_FUNCTION, UniqueFunction.UNIQUE_BY_YEAR_FUNCTION,
                    QueryFunctions.MOST_RECENT_PREFIX + QueryFunctions.UNIQUE_FUNCTION,
                    QueryFunctions.MOST_RECENT_PREFIX + UniqueFunction.UNIQUE_BY_DAY_FUNCTION,
                    QueryFunctions.MOST_RECENT_PREFIX + UniqueFunction.UNIQUE_BY_HOUR_FUNCTION,
                    QueryFunctions.MOST_RECENT_PREFIX + UniqueFunction.UNIQUE_BY_MINUTE_FUNCTION,
                    QueryFunctions.MOST_RECENT_PREFIX + UniqueFunction.UNIQUE_BY_TENTH_OF_HOUR_FUNCTION,
                    QueryFunctions.MOST_RECENT_PREFIX + UniqueFunction.UNIQUE_BY_MONTH_FUNCTION,
                    QueryFunctions.MOST_RECENT_PREFIX + UniqueFunction.UNIQUE_BY_SECOND_FUNCTION,
                    QueryFunctions.MOST_RECENT_PREFIX + UniqueFunction.UNIQUE_BY_MILLISECOND_FUNCTION,
                    QueryFunctions.MOST_RECENT_PREFIX + UniqueFunction.UNIQUE_BY_YEAR_FUNCTION, QueryFunctions.GROUPBY_FUNCTION,
                    QueryFunctions.EXCERPT_FIELDS_FUNCTION, QueryFunctions.SUMMARY_FUNCTION, QueryFunctions.NO_EXPANSION,
                    QueryFunctions.LENIENT_FIELDS_FUNCTION, QueryFunctions.STRICT_FIELDS_FUNCTION, QueryFunctions.SUM, QueryFunctions.MIN, QueryFunctions.MAX,
                    QueryFunctions.AVERAGE, QueryFunctions.COUNT, QueryFunctions.RENAME_FUNCTION);

    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T collect(T node, Object data) {
        QueryOptionsFromQueryVisitor visitor = new QueryOptionsFromQueryVisitor();
        return (T) node.jjtAccept(visitor, data);
    }

    /**
     * If the passed data is a {@link Map}, type cast and call the method to begin collection of the function arguments
     *
     * @param node
     *            potentially an f:options function node
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
     * If the passed OR node contains a f:options, descend the tree with a List in the passed userdata. The function args will be collected into the List when
     * visiting the child ASTStringLiteral nodes.
     *
     * @param node
     *            the {@link ASTOrNode}
     * @param data
     *            the data
     * @return the rebuilt node, or null if all children of the node were deleted
     */
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return visitJunction(node, data, () -> new ASTOrNode(JJTORNODE));
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        return visitJunction(node, data, () -> new ASTAndNode(JJTANDNODE));
    }

    private Object visitJunction(JexlNode node, Object data, Supplier<JexlNode> creator) {
        // Visit each child, and keep only the non-null ones.
        List<JexlNode> children = new ArrayList<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            Object copy = node.jjtGetChild(i).jjtAccept(this, data);
            if (copy != null) {
                children.add((JexlNode) copy);
            }
        }

        // If there are no children, return null and effectively delete the junction.
        if (children.isEmpty()) {
            return null;
        } else if (children.size() == 1) {
            // If there is one child, delete the junction and return the child.
            return children.get(0);
        } else {
            // If there are multiple children, return a new junction with the children.
            JexlNode copy = creator.get();
            JexlNodes.copyIdentifierOrLiteral(node, copy);
            JexlNodes.setChildren(copy, children.toArray(new JexlNode[0]));
            return copy;
        }
    }

    public enum UniqueFunction {
        UNIQUE_BY_DAY(UniqueFunction.UNIQUE_BY_DAY_FUNCTION, UniqueGranularity.TRUNCATE_TEMPORAL_TO_DAY),
        UNIQUE_BY_HOUR(UniqueFunction.UNIQUE_BY_HOUR_FUNCTION, UniqueGranularity.TRUNCATE_TEMPORAL_TO_HOUR),
        UNIQUE_BY_MILLISECOND(UniqueFunction.UNIQUE_BY_MILLISECOND_FUNCTION, UniqueGranularity.TRUNCATE_TEMPORAL_TO_MILLISECOND),
        UNIQUE_BY_MINUTE(UniqueFunction.UNIQUE_BY_MINUTE_FUNCTION, UniqueGranularity.TRUNCATE_TEMPORAL_TO_MINUTE),
        UNIQUE_BY_MONTH(UniqueFunction.UNIQUE_BY_MONTH_FUNCTION, UniqueGranularity.TRUNCATE_TEMPORAL_TO_MONTH),
        UNIQUE_BY_SECOND(UniqueFunction.UNIQUE_BY_SECOND_FUNCTION, UniqueGranularity.TRUNCATE_TEMPORAL_TO_SECOND),
        UNIQUE_BY_TENTH_OF_HOUR(UniqueFunction.UNIQUE_BY_TENTH_OF_HOUR_FUNCTION, UniqueGranularity.TRUNCATE_TEMPORAL_TO_TENTH_OF_HOUR),
        UNIQUE_BY_YEAR(UniqueFunction.UNIQUE_BY_YEAR_FUNCTION, UniqueGranularity.TRUNCATE_TEMPORAL_TO_YEAR);

        public static final String UNIQUE_BY_DAY_FUNCTION = "unique_by_day";
        public static final String UNIQUE_BY_HOUR_FUNCTION = "unique_by_hour";
        public static final String UNIQUE_BY_MINUTE_FUNCTION = "unique_by_minute";
        public static final String UNIQUE_BY_TENTH_OF_HOUR_FUNCTION = "unique_by_tenth_of_hour";
        public static final String UNIQUE_BY_MONTH_FUNCTION = "unique_by_month";
        public static final String UNIQUE_BY_SECOND_FUNCTION = "unique_by_second";
        public static final String UNIQUE_BY_MILLISECOND_FUNCTION = "unique_by_millisecond";
        public static final String UNIQUE_BY_YEAR_FUNCTION = "unique_by_year";

        public final String name;
        public final UniqueGranularity granularity;

        UniqueFunction(String name, UniqueGranularity granularity) {
            this.name = name;
            this.granularity = granularity;
        }

        public String getName() {
            return name;
        }

        public static UniqueFunction findByName(String name) {
            return UniqueFunction.valueOf(name.toUpperCase());
        }
    }

    private void updateUniqueFields(ASTFunctionNode node, UniqueFields uniqueFields, Map<String,String> optionsMap, UniqueFunction uniqueFunction) {
        putFieldsFromChildren(node, uniqueFields, uniqueFunction.granularity);
        updateUniqueFieldsOption(optionsMap, uniqueFields);
    }

    /**
     * If this is a function that contains key/value options, descend the tree with a {@link List} as the data. The function args will be collected into the
     * list when visiting the child {@link ASTStringLiteral} nodes.
     *
     * @param node
     *            the {@link ASTFunctionNode} that potentially contains parsable function options
     * @param optionsMap
     *            a {@link Map} that any parsed options key/values will be added to
     * @return null if this is a function with any key/value options, or the rebuilt node
     */
    private Object visit(ASTFunctionNode node, Map<String,String> optionsMap) {
        ASTNamespaceIdentifier nsIdentifier = (ASTNamespaceIdentifier) node.jjtGetChild(0);
        // if this is the f:options function, create a List for the userData to be passed to the child nodes
        if (nsIdentifier.getNamespace().equals(QueryFunctions.QUERY_FUNCTION_NAMESPACE)) {
            String function = String.valueOf(nsIdentifier.getName());

            // check for the most recent flag for the unique functions only
            boolean mostRecent = function.startsWith(QueryFunctions.MOST_RECENT_PREFIX + QueryFunctions.UNIQUE_FUNCTION);
            if (mostRecent) {
                function = function.substring(QueryFunctions.MOST_RECENT_PREFIX.length());
                optionsMap.put(QueryParameters.MOST_RECENT_UNIQUE, "true");
            }

            switch (function) {
                case QueryFunctions.OPTIONS_FUNCTION: {
                    List<String> optionsList = new ArrayList<>();
                    this.visit(node, optionsList);
                    // Parse the options List pairs into the map as key,value,key,value....
                    for (int i = 0; i + 1 < optionsList.size(); i++) {
                        String key = optionsList.get(i++);
                        String value = optionsList.get(i);
                        switch (key) {
                            case QueryParameters.UNIQUE_FIELDS:
                                updateUniqueFieldsOption(optionsMap, UniqueFields.from(value));
                                break;
                            case QueryParameters.GROUP_FIELDS:
                            case QueryParameters.EXCERPT_FIELDS:
                            case QueryParameters.NO_EXPANSION_FIELDS:
                            case QueryParameters.LENIENT_FIELDS:
                            case QueryParameters.STRICT_FIELDS:
                            case QueryParameters.RENAME_FIELDS:
                                updateFieldsOption(optionsMap, key, Collections.singletonList(value));
                                break;
                            default:
                                optionsMap.put(key, value);
                        }
                    }
                    return null;
                }
                case QueryFunctions.UNIQUE_FUNCTION: {
                    // Get the list of declared fields and join them into a comma-delimited string.
                    List<String> fieldList = new ArrayList<>();
                    this.visit(node, fieldList);
                    String fieldString = JOINER.join(fieldList);

                    // Parse the unique fields.
                    UniqueFields uniqueFields = UniqueFields.from(fieldString);
                    updateUniqueFieldsOption(optionsMap, uniqueFields);
                    return null;
                }
                case UniqueFunction.UNIQUE_BY_DAY_FUNCTION:
                case UniqueFunction.UNIQUE_BY_HOUR_FUNCTION:
                case UniqueFunction.UNIQUE_BY_MINUTE_FUNCTION:
                case UniqueFunction.UNIQUE_BY_MONTH_FUNCTION:
                case UniqueFunction.UNIQUE_BY_YEAR_FUNCTION:
                case UniqueFunction.UNIQUE_BY_MILLISECOND_FUNCTION:
                case UniqueFunction.UNIQUE_BY_SECOND_FUNCTION:
                case UniqueFunction.UNIQUE_BY_TENTH_OF_HOUR_FUNCTION: {
                    UniqueFields uniqueFields = new UniqueFields();
                    updateUniqueFields(node, uniqueFields, optionsMap, UniqueFunction.findByName(function));
                    return null;
                }
                case QueryFunctions.GROUPBY_FUNCTION: {
                    List<String> optionsList = new ArrayList<>();
                    this.visit(node, optionsList);
                    updateFieldsOption(optionsMap, QueryParameters.GROUP_FIELDS, optionsList);
                    return null;
                }
                case QueryFunctions.EXCERPT_FIELDS_FUNCTION: {
                    List<String> optionsList = new ArrayList<>();
                    this.visit(node, optionsList);
                    updateFieldsOption(optionsMap, QueryParameters.EXCERPT_FIELDS, optionsList);
                    return null;
                }
                case QueryFunctions.SUMMARY_FUNCTION: {
                    List<String> options = new ArrayList<>();
                    this.visit(node, options);
                    optionsMap.put(QueryParameters.SUMMARY_OPTIONS, JOINER.join(options));
                    return null;
                }
                case QueryFunctions.NO_EXPANSION: {
                    List<String> optionsList = new ArrayList<>();
                    this.visit(node, optionsList);
                    updateFieldsOption(optionsMap, QueryParameters.NO_EXPANSION_FIELDS, optionsList);
                    return null;
                }
                case QueryFunctions.LENIENT_FIELDS_FUNCTION: {
                    List<String> optionsList = new ArrayList<>();
                    this.visit(node, optionsList);
                    updateFieldsOption(optionsMap, QueryParameters.LENIENT_FIELDS, optionsList);
                    return null;
                }
                case QueryFunctions.STRICT_FIELDS_FUNCTION: {
                    List<String> optionsList = new ArrayList<>();
                    this.visit(node, optionsList);
                    updateFieldsOption(optionsMap, QueryParameters.STRICT_FIELDS, optionsList);
                    return null;
                }
                case QueryFunctions.SUM: {
                    List<String> options = new ArrayList<>();
                    this.visit(node, options);
                    optionsMap.put(QueryParameters.SUM_FIELDS, JOINER.join(options));
                    return null;
                }
                case QueryFunctions.MAX: {
                    List<String> options = new ArrayList<>();
                    this.visit(node, options);
                    optionsMap.put(QueryParameters.MAX_FIELDS, JOINER.join(options));
                    return null;
                }
                case QueryFunctions.MIN: {
                    List<String> options = new ArrayList<>();
                    this.visit(node, options);
                    optionsMap.put(QueryParameters.MIN_FIELDS, JOINER.join(options));
                    return null;
                }
                case QueryFunctions.AVERAGE: {
                    List<String> options = new ArrayList<>();
                    this.visit(node, options);
                    optionsMap.put(QueryParameters.AVERAGE_FIELDS, JOINER.join(options));
                    return null;
                }
                case QueryFunctions.COUNT: {
                    List<String> options = new ArrayList<>();
                    this.visit(node, options);
                    optionsMap.put(QueryParameters.COUNT_FIELDS, JOINER.join(options));
                    return null;
                }
                case QueryFunctions.RENAME_FUNCTION: {
                    List<String> optionsList = new ArrayList<>();
                    this.visit(node, optionsList);
                    updateFieldsOption(optionsMap, QueryParameters.RENAME_FIELDS, optionsList);
                    return null;
                }
            }
        }
        return super.visit(node, optionsMap);
    }

    // Find all unique fields declared in the provided node and add them to the provided {@link UniqueFields} with the specified transformer.
    private void putFieldsFromChildren(JexlNode node, UniqueFields uniqueFields, UniqueGranularity transformer) {
        List<String> fields = new ArrayList<>();
        node.jjtAccept(this, fields);
        fields.forEach((field) -> uniqueFields.put(field, transformer));
    }

    // Update the {@value QueryParameters#UNIQUE_FIELDS} option to include the given unique fields.
    private void updateUniqueFieldsOption(Map<String,String> optionsMap, UniqueFields uniqueFields) {
        // Combine with any previously found unique fields.
        if (optionsMap.containsKey(QueryParameters.UNIQUE_FIELDS)) {
            UniqueFields existingFields = UniqueFields.from(optionsMap.get(QueryParameters.UNIQUE_FIELDS));
            uniqueFields.putAll(existingFields.getFieldMap());
        }
        optionsMap.put(QueryParameters.UNIQUE_FIELDS, uniqueFields.toString());
    }

    // Update the option with the additional fields
    private void updateFieldsOption(Map<String,String> optionsMap, String option, List<String> fields) {
        // Combine with any previously found field lists
        if (optionsMap.containsKey(option)) {
            List<String> fieldsUnion = new ArrayList<>();
            fieldsUnion.addAll(fields);
            fieldsUnion.add(optionsMap.get(option));
            fields = fieldsUnion;
        }
        optionsMap.put(option, JOINER.join(fields));
    }

    /**
     * If the passed data is a {@link List}, add the node's image to the list.
     *
     * @param node
     *            the {@link ASTStringLiteral} node
     * @param data
     *            the data, possible a {@link List}
     * @return the rebuilt node
     */
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        addImageToListData(node, data);
        return super.visit(node, data);
    }

    /**
     * If the passed data is a {@link List}, and the node's image is not in the {@link #RESERVED} set, add the node's image to the list.
     *
     * @param node
     *            the {@link ASTIdentifier} node
     * @param data
     *            the data, possibly a {@link List}
     * @return the rebuilt node
     */
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        // Check if the current node is a child of the f:options function.
        if (!RESERVED.contains(JexlNodes.getIdentifierOrLiteral(node))) {
            addImageToListData(node, data);
        }
        return super.visit(node, data);
    }

    // If the given data is a list, add the node's image to it.
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void addImageToListData(JexlNode node, Object data) {
        if (data instanceof List) {
            List list = (List) data;
            list.add(JexlNodes.getIdentifierOrLiteral(node));
        }
    }

    /**
     * If the visit to the ASTFunction node returned null (because it was the options function) then there could be an empty ASTReference node. This would
     * generate a parseException in the jexl Parser unless the empty ASTReference node is also removed by returning null here.
     *
     * @param node
     *            the {@link ASTReference} node
     * @param data
     *            the data
     * @return the rebuilt node if it has children, or null otherwise
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
     *            the {@link ASTReferenceExpression} node
     * @param data
     *            the data
     * @return the rebuilt node if it has children, or null otherwise
     */
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        JexlNode n = (JexlNode) super.visit(node, data);
        if (n.jjtGetNumChildren() == 0)
            return null;
        return n;
    }

}
