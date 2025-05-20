package datawave.query.jexl.functions;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.UniqueFields;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.util.StringUtils;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public class QueryFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {

    public static final String BETWEEN = "between";
    public static final String LENGTH = "length";

    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     */
    public static class QueryJexlArgumentDescriptor implements JexlArgumentDescriptor {
        private final ASTFunctionNode node;
        private final String namespace;
        private final String name;
        private final List<JexlNode> args;

        public QueryJexlArgumentDescriptor(ASTFunctionNode node, String namespace, String name, List<JexlNode> args) {
            this.node = node;
            this.namespace = namespace;
            this.name = name;
            this.args = args;
        }

        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            switch (name) {
                case BETWEEN:
                    JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0),
                                    JexlNodes.getIdentifierOrLiteralAsString(args.get(1)));
                    JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0),
                                    JexlNodes.getIdentifierOrLiteralAsString(args.get(2)));
                    // Return a bounded range.
                    return QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode)), BOUNDED_RANGE);
                case LENGTH:
                    // Return a regex node with the appropriate number of matching characters
                    return JexlNodeFactory.buildNode(new ASTERNode(ParserTreeConstants.JJTERNODE), args.get(0),
                                    ".{" + JexlNodes.getIdentifierOrLiteral(args.get(1)) + ',' + JexlNodes.getIdentifierOrLiteral(args.get(2)) + '}');
                case QueryFunctions.MATCH_REGEX:
                    // Return an index query.
                    return getIndexQuery();
                case QueryFunctions.INCLUDE_TEXT:
                    // Return the appropriate index query.
                    return getTextIndexQuery(helper);
                default:
                    // Return the true node if unable to parse arguments.
                    return TRUE_NODE;
            }
        }

        private JexlNode getIndexQuery() {
            JexlNode node0 = args.get(0);
            final String value = JexlNodes.getIdentifierOrLiteralAsString(args.get(1));
            if (node0 instanceof ASTIdentifier) {
                final String field = JexlASTHelper.deconstructIdentifier(((ASTIdentifier) node0).getName());
                return JexlNodeFactory.buildNode((ASTERNode) null, field, value);
            } else {
                // node0 is an Or node or an And node
                // copy it
                JexlNode newParent = JexlNodeFactory.shallowCopy(node0);
                int i = 0;
                for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(node0)) {
                    String field = JexlASTHelper.deconstructIdentifier(identifier.getName());
                    JexlNode kid = JexlNodeFactory.buildNode((ASTERNode) null, field, value);
                    kid.jjtSetParent(newParent);
                    newParent.jjtAddChild(kid, i++);
                }
                return newParent;
            }
        }

        /**
         * The index query for a text function MUST normalize the value as the actual value may differ between the event key and the index key
         *
         * @param helper
         *            a metadata helper
         * @return a JexlNode
         */
        private JexlNode getTextIndexQuery(MetadataHelper helper) {
            List<JexlNode> children = new LinkedList<>();

            if (args.size() == 2) {
                // single field value
                createChildren(children, args.get(0), args.get(1), helper);
            } else if (args.size() % 2 == 1) {
                // dealing with {AND/OR, field, value, field value}
                for (int i = 1; i < args.size(); i += 2) {
                    createChildren(children, args.get(i), args.get(i + 1), helper);
                }
            }

            switch (children.size()) {
                case 0:
                    return null;
                case 1:
                    return children.get(0);
                default:
                    // expand into an OR, unless an intersection is specifically requested
                    String expansion = JexlASTHelper.getIdentifier(args.get(0));
                    if (expansion.equals("AND")) {
                        return JexlNodeFactory.createAndNode(children);
                    } else {
                        return JexlNodeFactory.createOrNode(children);
                    }
            }
        }

        private void createChildren(List<JexlNode> children, JexlNode fieldName, JexlNode fieldValue, MetadataHelper helper) {
            String field = JexlASTHelper.deconstructIdentifier(((ASTIdentifier) fieldName).getName());
            String literal = JexlNodes.getIdentifierOrLiteralAsString(fieldValue);
            Set<String> values = getNormalizedValues(field, literal, helper);
            for (String value : values) {
                children.add(JexlNodeFactory.buildNode((ASTEQNode) null, field, value));
            }
        }

        private Set<String> getNormalizedValues(String field, String value, MetadataHelper helper) {
            Set<String> values = new HashSet<>();
            values.add(value); // retain original

            Set<Type<?>> types = getTypesForField(field, helper);
            for (Type<?> type : types) {
                try {
                    values.add(type.normalize(value));
                } catch (IllegalArgumentException e) {
                    // failure to normalize is not a problem
                }
            }
            return values;
        }

        private Set<Type<?>> getTypesForField(String field, MetadataHelper helper) {
            try {
                if (field.equals(Constants.ANY_FIELD)) {
                    return helper.getAllDatatypes();
                } else {
                    return helper.getDatatypesForField(field);
                }
            } catch (InstantiationException | TableNotFoundException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }

        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // Do not normalize fields for the includeText function.
            if (!name.equalsIgnoreCase(QueryFunctions.INCLUDE_TEXT)) {
                // All other functions use the fields in the first argument for normalization.
                if (arg > 0) {
                    return fields(helper, datatypeFilter);
                }
            }
            return Collections.emptySet();
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            Set<String> fields = new HashSet<>();
            switch (name) {
                case QueryFunctions.COUNT:
                case QueryFunctions.SUM:
                case QueryFunctions.MIN:
                case QueryFunctions.MAX:
                case QueryFunctions.AVERAGE:
                case QueryFunctions.GROUPBY_FUNCTION:
                case QueryFunctions.NO_EXPANSION:
                case QueryFunctions.LENIENT_FIELDS_FUNCTION:
                case QueryFunctions.STRICT_FIELDS_FUNCTION:
                case QueryFunctions.EXCERPT_FIELDS_FUNCTION:
                case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_YEAR_FUNCTION:
                case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MONTH_FUNCTION:
                case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_DAY_FUNCTION:
                case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_HOUR_FUNCTION:
                case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_TENTH_OF_HOUR_FUNCTION:
                case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MINUTE_FUNCTION:
                case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_SECOND_FUNCTION:
                case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MILLISECOND_FUNCTION:
                    // In practice each of these functions should be parsed from the query
                    // almost immediately. This implementation is added for consistency
                    for (JexlNode arg : args) {
                        fields.addAll(JexlASTHelper.getIdentifierNames(arg));
                    }
                    break;
                case QueryFunctions.INCLUDE_TEXT:
                    if (args.size() == 2) {
                        fields.addAll(JexlASTHelper.getIdentifierNames(args.get(0)));
                    } else {
                        for (int i = 1; i < args.size(); i += 2) {
                            fields.addAll(JexlASTHelper.getIdentifierNames(args.get(i)));
                        }
                    }
                    break;
                case QueryFunctions.UNIQUE_FUNCTION:
                    for (JexlNode arg : args) {
                        if (arg instanceof ASTStringLiteral) {
                            // FIELD[GRANULARITY] is represented by an ASTStringLiteral
                            String literal = ((ASTStringLiteral) arg).getLiteral();
                            fields.addAll(UniqueFields.from(literal).getFields());
                        } else {
                            // otherwise it's just an ASTIdentifier
                            for (String identifier : JexlASTHelper.getIdentifierNames(arg)) {
                                fields.addAll(UniqueFields.from(identifier).getFields());
                            }
                        }
                    }
                    break;
                case QueryFunctions.SUMMARY_FUNCTION:
                    break;
                case QueryFunctions.RENAME_FUNCTION:
                    for (JexlNode arg : args) {
                        String value = JexlNodes.getIdentifierOrLiteralAsString(arg);
                        String[] parts = StringUtils.split(value, Constants.EQUALS);
                        fields.add(parts[0]);
                    }
                case QueryFunctions.MATCH_REGEX:
                case BETWEEN:
                case LENGTH:
                default:
                    fields.addAll(JexlASTHelper.getIdentifierNames(args.get(0)));
            }
            return fields;
        }

        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            Set<Set<String>> fieldSet = new HashSet<>();
            Set<String> fields = fields(helper, datatypeFilter);
            for (String field : fields) {
                fieldSet.add(Set.of(field));
            }
            return fieldSet;
        }

        @Override
        public boolean useOrForExpansion() {
            return true;
        }

        @Override
        public boolean regexArguments() {
            return true;
        }

        @Override
        public boolean allowIvaratorFiltering() {
            return true;
        }
    }

    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor visitor = FunctionJexlNodeVisitor.eval(node);
        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(visitor.namespace());

        if (!QueryFunctions.QUERY_FUNCTION_NAMESPACE.equals(visitor.namespace())) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NAMESPACE_UNEXPECTED,
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of " + visitor.namespace());
            throw new IllegalArgumentException(qe);
        }
        if (!functionClass.equals(QueryFunctions.class)) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NODE_FOR_FUNCTION,
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in " + functionClass);
            throw new IllegalArgumentException(qe);
        }
        verify(visitor.name(), visitor.args().size());

        return new QueryJexlArgumentDescriptor(node, visitor.namespace(), visitor.name(), visitor.args());
    }

    private static void verify(String name, int numArgs) {
        switch (name) {
            case BETWEEN:
                if (numArgs != 3) {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.WRONG_NUMBER_OF_ARGUMENTS,
                                    "Wrong number of arguments to between function");
                    throw new IllegalArgumentException(qe);
                }
                break;
            case LENGTH:
                if (numArgs != 3) {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.WRONG_NUMBER_OF_ARGUMENTS,
                                    "Wrong number of arguments to length function");
                    throw new IllegalArgumentException(qe);
                }
                break;
            case QueryFunctions.OPTIONS_FUNCTION:
                if (numArgs % 2 != 0) {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.WRONG_NUMBER_OF_ARGUMENTS,
                                    "Expected even number of arguments to options function");
                    throw new IllegalArgumentException(qe);
                }
                break;
            case QueryFunctions.UNIQUE_FUNCTION:
            case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MILLISECOND_FUNCTION:
            case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_SECOND_FUNCTION:
            case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MINUTE_FUNCTION:
            case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_TENTH_OF_HOUR_FUNCTION:
            case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_HOUR_FUNCTION:
            case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_DAY_FUNCTION:
            case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MONTH_FUNCTION:
            case QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_YEAR_FUNCTION:
            case QueryFunctions.MOST_RECENT_PREFIX + QueryFunctions.UNIQUE_FUNCTION:
            case QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MILLISECOND_FUNCTION:
            case QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_SECOND_FUNCTION:
            case QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MINUTE_FUNCTION:
            case QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_TENTH_OF_HOUR_FUNCTION:
            case QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_HOUR_FUNCTION:
            case QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_DAY_FUNCTION:
            case QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MONTH_FUNCTION:
            case QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_YEAR_FUNCTION:
            case QueryFunctions.GROUPBY_FUNCTION:
            case QueryFunctions.EXCERPT_FIELDS_FUNCTION:
            case QueryFunctions.MATCH_REGEX:
            case QueryFunctions.INCLUDE_TEXT:
            case QueryFunctions.NO_EXPANSION:
            case QueryFunctions.LENIENT_FIELDS_FUNCTION:
            case QueryFunctions.STRICT_FIELDS_FUNCTION:
            case QueryFunctions.SUM:
            case QueryFunctions.COUNT:
            case QueryFunctions.MIN:
            case QueryFunctions.MAX:
            case QueryFunctions.AVERAGE:
            case QueryFunctions.RENAME_FUNCTION:
                if (numArgs == 0) {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.WRONG_NUMBER_OF_ARGUMENTS,
                                    "Expected at least one argument to the " + name + " function");
                    throw new IllegalArgumentException(qe);
                }
                break;
            case QueryFunctions.SUMMARY_FUNCTION:
                break;
            default:
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FUNCTION_NOT_FOUND, "Unknown Query function: " + name);
                throw new IllegalArgumentException(qe);
        }
    }
}
