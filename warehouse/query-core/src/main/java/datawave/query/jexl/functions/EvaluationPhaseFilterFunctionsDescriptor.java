package datawave.query.jexl.functions;

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
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
                final String beginArg = (arguments.size() == 5 ? arguments.get(2).image : arguments.get(1).image);
                final String endArg = (arguments.size() == 5 ? arguments.get(3).image : arguments.get(2).image);
                Date begin, end = null;
                if (arguments.size() >= 4) {
                    String formatArg = arguments.get(arguments.size() - 1).image;
                    DateFormat formatter = EvaluationPhaseFilterFunctions.newSimpleDateFormat(formatArg);
                    begin = new Date(EvaluationPhaseFilterFunctions.getTime(beginArg, formatter));
                    end = new Date(EvaluationPhaseFilterFunctions.getTime(endArg, formatter));
                } else {
                    begin = new Date(EvaluationPhaseFilterFunctions.getTime(beginArg));
                    end = new Date(EvaluationPhaseFilterFunctions.getTime(endArg));
                }
                if (node0 instanceof ASTIdentifier) {
                    
                    final String field = JexlASTHelper.deconstructIdentifier(node0.image);
                    
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
                        String field = JexlASTHelper.deconstructIdentifier(identifier.image);
                        
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
                throw new IllegalArgumentException("Unable to parse dates from date function", e);
            } catch (TableNotFoundException e) {
                // if we are missing the table, then lets assume the date index is simply not configured on this system
                log.warn("Missing date index, scanning entire range", e);
                return TRUE_NODE;
            }
        }
        
        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap)
                        throws TableNotFoundException, InstantiationException, IllegalAccessException {
            // Since the getIndexQuery does not supply actual filters on the fields, we need to add those filters here
            Set<String> queryFields = fields(null, null);
            // argument 0 (child 2) is the fieldname, argument 1 (child 3) is the regex
            String regex = (regexArguments() ? JexlASTHelper.getLiteral(node.jjtGetChild(3)).image : null);
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
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) throws TableNotFoundException, InstantiationException,
                        IllegalAccessException {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            Set<String> fields = Sets.newHashSet();
            
            List<JexlNode> arguments = functionMetadata.args();
            if (MATCHCOUNTOF.equals(functionMetadata.name())) {
                fields.addAll(filterFields(JexlASTHelper.getIdentifierNames(arguments.get(1)), helper, datatypeFilter));
            } else if (TIMEFUNCTION.equals(functionMetadata.name())) {
                fields.addAll(filterFields(JexlASTHelper.getIdentifierNames(arguments.get(0)), helper, datatypeFilter));
                fields.addAll(filterFields(JexlASTHelper.getIdentifierNames(arguments.get(1)), helper, datatypeFilter));
            } else if (COMPARE.equals(functionMetadata.name())) {
                fields.addAll(filterFields(JexlASTHelper.getIdentifierNames(arguments.get(0)), helper, datatypeFilter));
                fields.addAll(filterFields(JexlASTHelper.getIdentifierNames(arguments.get(3)), helper, datatypeFilter));
            } else {
                fields.addAll(filterFields(JexlASTHelper.getIdentifierNames(arguments.get(0)), helper, datatypeFilter));
            }
            return fields;
        }
        
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) throws TableNotFoundException, InstantiationException,
                        IllegalAccessException {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            
            List<JexlNode> arguments = functionMetadata.args();
            if (MATCHCOUNTOF.equals(functionMetadata.name())) {
                return filterSets(JexlArgumentDescriptor.Fields.product(arguments.get(1)), helper, datatypeFilter);
            } else if (TIMEFUNCTION.equals(functionMetadata.name())) {
                return filterSets(JexlArgumentDescriptor.Fields.product(arguments.get(0), arguments.get(1)), helper, datatypeFilter);
            } else if (COMPARE.equals(functionMetadata.name())) {
                return filterSets(JexlArgumentDescriptor.Fields.product(arguments.get(0), arguments.get(3)), helper, datatypeFilter);
            } else {
                return filterSets(JexlArgumentDescriptor.Fields.product(arguments.get(0)), helper, datatypeFilter);
            }
        }
        
        private Set<String> filterFields(Set<String> fields, MetadataHelper helper, Set<String> datatypeFilter) throws TableNotFoundException,
                        InstantiationException, IllegalAccessException {
            Set<String> filteredFields = new HashSet<>();
            if (datatypeFilter == null) {
                filteredFields.addAll(fields);
            } else if (!datatypeFilter.isEmpty()) {
                for (String field : fields) {
                    if (!helper.getDatatypesForField(field, datatypeFilter).isEmpty()) {
                        filteredFields.add(field);
                    }
                }
            } // non-null but empty typeFilters allow nothing
            return Collections.unmodifiableSet(filteredFields);
        }
        
        private Set<Set<String>> filterSets(Set<Set<String>> sets, MetadataHelper helper, Set<String> datatypeFilter) throws TableNotFoundException,
                        InstantiationException, IllegalAccessException {
            Set<Set<String>> filteredSets = new HashSet<>();
            for (Set<String> set : sets) {
                filteredSets.add(filterFields(set, helper, datatypeFilter));
            }
            return Collections.unmodifiableSet(filteredSets);
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
                throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with node for a function in " + clazz);
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
