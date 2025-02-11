package datawave.query.jexl.functions;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Evaluation phase filter functions cannot be evaluated against index-only fields
 */
public class EvaluationPhaseFilterFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {

    public static final String EXCLUDE_REGEX = "excludeRegex";
    public static final String INCLUDE_REGEX = "includeRegex";
    public static final String IS_NULL = "isNull";
    public static final String BETWEEN_DATES = "betweenDates";
    public static final String BETWEEN_LOAD_DATES = "betweenLoadDates";
    public static final String MATCHES_AT_LEAST_COUNT_OF = "matchesAtLeastCountOf";
    public static final String TIME_FUNCTION = "timeFunction";
    public static final String COMPARE = "compare";
    public static final String GET_ALL_MATCHES = "getAllMatches";

    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     *
     *
     */
    public static class EvaluationPhaseFilterJexlArgumentDescriptor implements JexlArgumentDescriptor {
        private static final Logger log = Logger.getLogger(EvaluationPhaseFilterJexlArgumentDescriptor.class);

        public static final ImmutableSet<String> regexFunctions = ImmutableSet.of(EXCLUDE_REGEX, INCLUDE_REGEX, GET_ALL_MATCHES);
        public static final ImmutableSet<String> andExpansionFunctions = ImmutableSet.of(IS_NULL);
        public static final ImmutableSet<String> dateBetweenFunctions = ImmutableSet.of(BETWEEN_DATES, BETWEEN_LOAD_DATES);
        public static final String MATCHCOUNTOF = MATCHES_AT_LEAST_COUNT_OF;
        public static final String TIMEFUNCTION = TIME_FUNCTION;
        private final ASTFunctionNode node;

        public EvaluationPhaseFilterJexlArgumentDescriptor(ASTFunctionNode node) {
            this.node = node;
        }

        /**
         * Returns 'true' for most of these functions because they are evaluation phase filter functions. However in a couple of special cases we add an index
         * query: 1) betweenDates and betweenLoadDates: we add a SHARDS_AND_DAYS statement 2) text: we add the index query but the function does not use the
         * indexed values
         */
        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            // in the special case of date functions and then only with the between methods,
            // we want to add a range stream hint in terms of SHARDS_AND_DAYS
            if (dateBetweenFunctions.contains(functionMetadata.name()) && dateIndexHelper != null) {
                return getShardsAndDaysQuery(functionMetadata, config, helper, dateIndexHelper, datatypeFilter);
            }

            // 'true' is returned to imply that there is no range lookup possible for this function
            return TRUE_NODE;
        }

        private JexlNode getShardsAndDaysQuery(FunctionJexlNodeVisitor functionMetadata, ShardQueryConfiguration config, MetadataHelper helper,
                        DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            try {
                List<JexlNode> arguments = functionMetadata.args();
                JexlNode node0 = arguments.get(0);
                final String beginArg = arguments.size() == 5 ? JexlNodes.getIdentifierOrLiteralAsString(arguments.get(2))
                                : JexlNodes.getIdentifierOrLiteralAsString(arguments.get(1));
                final String endArg = arguments.size() == 5 ? JexlNodes.getIdentifierOrLiteralAsString(arguments.get(3))
                                : JexlNodes.getIdentifierOrLiteralAsString(arguments.get(2));
                Date begin, end = null;
                if (arguments.size() >= 4) {
                    String formatArg = JexlNodes.getIdentifierOrLiteralAsString(arguments.get(arguments.size() - 1));
                    DateFormat formatter = EvaluationPhaseFilterFunctions.newSimpleDateFormat(formatArg);
                    begin = new Date(EvaluationPhaseFilterFunctions.getTime(beginArg, formatter));
                    end = new Date(EvaluationPhaseFilterFunctions.getTime(endArg, formatter));
                } else {
                    begin = new Date(EvaluationPhaseFilterFunctions.getTime(beginArg));
                    end = new Date(EvaluationPhaseFilterFunctions.getTime(endArg));
                }
                if (node0 instanceof ASTIdentifier) {

                    final String field = JexlASTHelper.deconstructIdentifier(((ASTIdentifier) node0).getName());

                    if (log.isDebugEnabled()) {
                        log.debug("Evaluating date index with " + field + ", [" + begin + "," + end + "], " + datatypeFilter);
                    }
                    String shardsAndDaysHint = dateIndexHelper.getShardsAndDaysHint(field, begin, end, config.getBeginDate(), config.getEndDate(),
                                    datatypeFilter);
                    // if we did not get any shards or days, then we have a field that is not indexed for the specified datatypes
                    if (shardsAndDaysHint == null || shardsAndDaysHint.isEmpty()) {
                        log.info("Found no index for field " + field + " and data types " + datatypeFilter);
                        return TRUE_NODE;
                    }
                    // create an assignment node
                    log.info("Found index for field " + field + " and data types " + datatypeFilter);
                    if (log.isTraceEnabled()) {
                        log.info("Found the following shards and days: " + shardsAndDaysHint);
                    }
                    return JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(Constants.SHARD_DAY_HINT, shardsAndDaysHint));
                } else {
                    // node0 is an Or node or an And node
                    // copy it
                    JexlNode newParent = JexlNodeFactory.shallowCopy(node0);
                    int i = 0;
                    for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(node0)) {
                        String field = JexlASTHelper.deconstructIdentifier(identifier.getName());

                        if (log.isDebugEnabled()) {
                            log.debug("Evaluating date index with " + field + ", [" + begin + "," + end + "], " + datatypeFilter);
                        }
                        String shardsAndDaysHint = dateIndexHelper.getShardsAndDaysHint(field, begin, end, config.getBeginDate(), config.getEndDate(),
                                        datatypeFilter);
                        // if we did not get any shards or days, then we have a field that is not indexed for the specified datatypes
                        if (shardsAndDaysHint == null || shardsAndDaysHint.isEmpty()) {
                            log.info("Found no index for field " + field + " and data types " + datatypeFilter);
                            // if an or node, then basically we need to search the entire range
                            if (newParent instanceof ASTOrNode) {
                                return TRUE_NODE;
                            }
                            // if an and node, then simply drop this one out of the mix
                        } else {
                            // create an assignment node
                            log.info("Found index for field " + field + " and data types " + datatypeFilter);
                            if (log.isTraceEnabled()) {
                                log.info("Found the following shards and days: " + shardsAndDaysHint);
                            }
                            JexlNode kid = JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(Constants.SHARD_DAY_HINT, shardsAndDaysHint));
                            kid.jjtSetParent(newParent);
                            newParent.jjtAddChild(kid, i++);
                        }
                    }
                    return newParent;

                }
            } catch (ParseException e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_DATE, "Unable to parse dates from date function");
                throw new IllegalArgumentException(qe);
            } catch (TableNotFoundException e) {
                // if we are missing the table, then lets assume the date index is simply not configured on this system
                log.warn("Missing date index, scanning entire range", e);
                return TRUE_NODE;
            }
        }

        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // Since the getIndexQuery does not supply actual filters on the fields, we need to add those filters here
            Set<String> queryFields = fields(null, null);
            // argument 0 (child 2) is the fieldname, argument 1 (child 3) is the regex
            ASTArguments argsNode = (ASTArguments) node.jjtGetChild(1);
            String regex = (regexArguments() ? ((ASTStringLiteral) argsNode.jjtGetChild(1)).getLiteral() : null);
            for (String fieldName : queryFields) {
                EventDataQueryExpressionVisitor.ExpressionFilter f = filterMap.get(fieldName);
                if (f == null) {
                    filterMap.put(fieldName, f = new EventDataQueryExpressionVisitor.ExpressionFilter(attributeFactory, fieldName));
                }
                if (regex == null) {
                    f.acceptAllValues();
                } else {
                    f.addFieldPattern(regex);
                }
            }
        }

        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // we do not want to normalize any of the regex arguments, nor any of the date arguments.
            return Collections.emptySet();
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            // Maintain insertion order.
            Set<String> fields = Sets.newLinkedHashSet();

            List<JexlNode> arguments = functionMetadata.args();
            if (MATCHCOUNTOF.equals(functionMetadata.name())) {
                fields.addAll(JexlASTHelper.getIdentifierNames(arguments.get(1)));
            } else if (TIMEFUNCTION.equals(functionMetadata.name())) {
                fields.addAll(JexlASTHelper.getIdentifierNames(arguments.get(0)));
                fields.addAll(JexlASTHelper.getIdentifierNames(arguments.get(1)));
            } else if (COMPARE.equals(functionMetadata.name())) {
                fields.addAll(JexlASTHelper.getIdentifierNames(arguments.get(0)));
                fields.addAll(JexlASTHelper.getIdentifierNames(arguments.get(3)));
            } else {
                fields.addAll(JexlASTHelper.getIdentifierNames(arguments.get(0)));
            }
            return fields;
        }

        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);

            List<JexlNode> arguments = functionMetadata.args();
            if (MATCHCOUNTOF.equals(functionMetadata.name())) {
                return JexlArgumentDescriptor.Fields.product(arguments.get(1));
            } else if (TIMEFUNCTION.equals(functionMetadata.name())) {
                return JexlArgumentDescriptor.Fields.product(arguments.get(0), arguments.get(1));
            } else if (COMPARE.equals(functionMetadata.name())) {
                return JexlArgumentDescriptor.Fields.product(arguments.get(0), arguments.get(3));
            } else {
                return JexlArgumentDescriptor.Fields.product(arguments.get(0));
            }
        }

        @Override
        public boolean useOrForExpansion() {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            return !andExpansionFunctions.contains(functionMetadata.name());
        }

        @Override
        public boolean regexArguments() {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            return regexFunctions.contains(functionMetadata.name());
        }

        @Override
        public boolean allowIvaratorFiltering() {
            return true;
        }
    }

    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        try {
            Class<?> clazz = GetFunctionClass.get(node);
            if (!EvaluationPhaseFilterFunctions.class.equals(clazz)) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.ARGUMENTDESCRIPTOR_NODE_FOR_FUNCTION,
                                "Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with node for a function in " + clazz);
                throw new IllegalArgumentException(qe);
            }
            FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
            fvis.visit(node, null);

            verify(fvis);

            return new EvaluationPhaseFilterJexlArgumentDescriptor(node);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void verify(FunctionJexlNodeVisitor fvis) {
        if (COMPARE.equalsIgnoreCase(fvis.name())) {
            CompareFunctionValidator.validate(fvis.name(), fvis.args());
        }
    }
}
