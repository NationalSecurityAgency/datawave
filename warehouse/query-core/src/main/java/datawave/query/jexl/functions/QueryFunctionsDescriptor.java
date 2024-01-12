package datawave.query.jexl.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

public class QueryFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    private static final Logger log = Logger.getLogger(QueryFunctionsDescriptor.class);

    public static final String BETWEEN = "between";
    public static final String LENGTH = "length";
    public static final String INCLUDE_TEXT = "includeText";

    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     */
    public static class QueryJexlArgumentDescriptor implements JexlArgumentDescriptor {
        private final ASTFunctionNode node;
        private final String namespace, name;
        private final List<JexlNode> args;

        public QueryJexlArgumentDescriptor(ASTFunctionNode node, String namespace, String name, List<JexlNode> args) {
            this.node = node;
            this.namespace = namespace;
            this.name = name;
            this.args = args;
        }

        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            try {
                Set<String> allFields = helper.getAllFields(datatypeFilter);
                switch (name) {
                    case BETWEEN:
                        JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), args.get(1).image);
                        JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), args.get(2).image);
                        // Return a bounded range.
                        return BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode)));
                    case LENGTH:
                        // Return a regex node with the appropriate number of matching characters
                        return JexlNodeFactory.buildNode(new ASTERNode(ParserTreeConstants.JJTERNODE), args.get(0),
                                        ".{" + args.get(1).image + ',' + args.get(2).image + '}');
                    case QueryFunctions.MATCH_REGEX:
                        // Return an index query.
                        return getIndexQuery(allFields);
                    case INCLUDE_TEXT:
                        // Return the appropriate index query.
                        return getTextIndexQuery(allFields);
                    default:
                        // Return the true node if unable to parse arguments.
                        return TRUE_NODE;
                }
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }
        }

        private JexlNode getIndexQuery(Set<String> allFields) throws TableNotFoundException {
            JexlNode node0 = args.get(0);
            final String value = args.get(1).image;
            if (node0 instanceof ASTIdentifier) {
                final String field = JexlASTHelper.deconstructIdentifier(node0.image);
                if (allFields.contains(field) || field.equals(Constants.ANY_FIELD)) {
                    return JexlNodeFactory.buildNode((ASTERNode) null, field, value);
                } else {
                    return null;
                }
            } else {
                // node0 is an Or node or an And node
                // copy it
                JexlNode newParent = JexlNodeFactory.shallowCopy(node0);
                int i = 0;
                for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(node0)) {
                    String field = JexlASTHelper.deconstructIdentifier(identifier.image);
                    if (allFields.contains(field)) {
                        JexlNode kid = JexlNodeFactory.buildNode((ASTERNode) null, field, value);
                        kid.jjtSetParent(newParent);
                        newParent.jjtAddChild(kid, i++);
                    }
                }
                return newParent;
            }
        }

        private JexlNode getTextIndexQuery(Set<String> allFields) {
            JexlNode node0 = args.get(0);
            final String value = args.get(1).image;
            if (node0 instanceof ASTIdentifier) {
                final String field = JexlASTHelper.deconstructIdentifier(node0.image);
                if (allFields.contains(field) || field.equals(Constants.ANY_FIELD)) {
                    return JexlNodeFactory.buildNode((ASTEQNode) null, field, value);
                } else {
                    return null;
                }
            } else {
                // node0 is an Or node or an And node
                // copy it
                JexlNode newParent = JexlNodeFactory.shallowCopy(node0);
                int i = 0;
                for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(node0)) {
                    String field = JexlASTHelper.deconstructIdentifier(identifier.image);
                    JexlNode kid = JexlNodeFactory.buildNode((ASTEQNode) null, field, value);
                    kid.jjtSetParent(newParent);
                    newParent.jjtAddChild(kid, i++);
                }
                return newParent;
            }
        }

        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }

        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // Do not normalize fields for the includeText function.
            if (!name.equalsIgnoreCase(INCLUDE_TEXT)) {
                // All other functions use the fields in the first argument for normalization.
                if (arg > 0) {
                    return fields(helper, datatypeFilter);
                }
            }
            return Collections.emptySet();
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            try {
                Set<String> allFields = helper.getAllFields(datatypeFilter);
                Set<String> filteredFields = Sets.newHashSet();

                for (String field : JexlASTHelper.getIdentifierNames(args.get(0))) {
                    filterField(allFields, field, filteredFields);
                }

                return filteredFields;
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                log.error(qe);
                throw new DatawaveFatalQueryException(qe);
            }
        }

        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            try {
                Set<Set<String>> filteredSets = Sets.newHashSet(Sets.newHashSet());
                Set<String> allFields = helper.getAllFields(datatypeFilter);

                for (Set<String> aFieldSet : JexlArgumentDescriptor.Fields.product(args.get(0))) {
                    filteredSets.add(filterSet(allFields, aFieldSet));
                }

                return filteredSets;
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                log.error(qe);
                throw new DatawaveFatalQueryException(qe);
            }
        }

        /**
         * Given a list of all possible fields, filters out fields based on the given datatype(s)
         *
         * @param allFields
         * @param fieldToAdd
         * @param returnedFields
         */
        private void filterField(Set<String> allFields, String fieldToAdd, Set<String> returnedFields) {
            if (allFields.contains(fieldToAdd)) {
                returnedFields.add(fieldToAdd);
            }
        }

        /**
         * Given a list of all possible fields, filters out fields based on the given datatype(s)
         *
         * @param allFields
         * @param fields
         */
        private Set<String> filterSet(Set<String> allFields, Set<String> fields) {
            Set<String> returnedFields = Sets.newHashSet();
            returnedFields.addAll(allFields);
            returnedFields.retainAll(fields);
            return returnedFields;
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

        if (!QueryFunctions.QUERY_FUNCTION_NAMESPACE.equals(node.jjtGetChild(0).image))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of "
                            + node.jjtGetChild(0).image);
        if (!functionClass.equals(QueryFunctions.class))
            throw new IllegalArgumentException(
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in " + functionClass);

        verify(visitor.name(), visitor.args().size());

        return new QueryJexlArgumentDescriptor(node, visitor.namespace(), visitor.name(), visitor.args());
    }

    private static void verify(String name, int numArgs) {
        switch (name) {
            case BETWEEN:
                if (numArgs != 3) {
                    throw new IllegalArgumentException("Wrong number of arguments to between function");
                }
                break;
            case LENGTH:
                if (numArgs != 3) {
                    throw new IllegalArgumentException("Wrong number of arguments to length function");
                }
                break;
            case QueryFunctions.OPTIONS_FUNCTION:
                if (numArgs % 2 != 0) {
                    throw new IllegalArgumentException("Expected even number of arguments to options function");
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
                if (numArgs == 0) {
                    throw new IllegalArgumentException("Expected at least one argument to the " + name + " function");
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown Query function: " + name);
        }
    }
}
