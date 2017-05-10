package datawave.query.rewrite.jexl.functions;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import datawave.query.rewrite.Constants;
import datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import datawave.query.rewrite.jexl.JexlASTHelper;
import datawave.query.rewrite.jexl.JexlNodeFactory;
import datawave.query.rewrite.jexl.functions.arguments.RefactoredJexlArgumentDescriptor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.*;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class EvaluationPhaseFilterFunctionsDescriptor implements RefactoredJexlFunctionArgumentDescriptorFactory {
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     * 
     *
     */
    public static class EvaluationPhaseFilterJexlArgumentDescriptor implements RefactoredJexlArgumentDescriptor {
        private static final Logger log = Logger.getLogger(EvaluationPhaseFilterJexlArgumentDescriptor.class);
        private static final ImmutableSet<String> regexFunctions = ImmutableSet.of("excludeRegex", "includeRegex");
        private static final ImmutableSet<String> andExpansionFunctions = ImmutableSet.of("isNull");
        private static final ImmutableSet<String> dateComparisonFunctions = ImmutableSet.of("afterDate", "beforeDate", "betweenDates", "afterLoadDate",
                        "beforeLoadDate", "betweenLoadDates");
        private static final ImmutableSet<String> dateBetweenFunctions = ImmutableSet.of("betweenDates", "betweenLoadDates");
        
        private final ASTFunctionNode node;
        
        public EvaluationPhaseFilterJexlArgumentDescriptor(ASTFunctionNode node) {
            this.node = node;
        }
        
        /**
         * Returns 'true' because none of these functions should influence the index query.
         */
        @Override
        public JexlNode getIndexQuery(RefactoredShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper,
                        Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            // in the special case of date functions and then only with the between methods,
            // we want to add a range stream hint in terms of SHARDS_AND_DAYS
            if (dateBetweenFunctions.contains(functionMetadata.name()) && dateIndexHelper != null) {
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
            
            // 'true' is returned to imply that there is no range lookup possible for this function
            return TRUE_NODE;
        }
        
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // we do not want to normalize any of the regex arguments, nor any of the date arguments.
            Set<String> fields = Collections.emptySet();
            return fields;
        }
        
        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            Set<String> fields = Sets.newHashSet();
            
            if (regexFunctions.contains(functionMetadata.name())) {
                // magic happens here
                List<JexlNode> arguments = functionMetadata.args();
                
                if (arguments.size() == 2) {
                    fields.addAll(JexlASTHelper.getIdentifierNames(arguments.get(0)));
                }
            } else if (dateComparisonFunctions.contains(functionMetadata.name())) {
                List<JexlNode> arguments = functionMetadata.args();
                if (arguments.size() >= 2) {
                    fields.addAll(JexlASTHelper.getIdentifierNames(arguments.get(0)));
                }
            } else if (andExpansionFunctions.contains(functionMetadata.name())) {
                List<JexlNode> arguments = functionMetadata.args();
                if (arguments.size() == 1) {
                    fields.addAll(JexlASTHelper.getIdentifierNames(arguments.get(0)));
                }
            }
            return fields;
        }
        
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            
            if (regexFunctions.contains(functionMetadata.name())) {
                // magic happens here
                List<JexlNode> arguments = functionMetadata.args();
                
                if (arguments.size() == 2) {
                    return RefactoredJexlArgumentDescriptor.Fields.product(arguments.get(0));
                }
            } else if (dateComparisonFunctions.contains(functionMetadata.name())) {
                List<JexlNode> arguments = functionMetadata.args();
                if (arguments.size() >= 2) {
                    return RefactoredJexlArgumentDescriptor.Fields.product(arguments.get(0));
                }
            } else if (andExpansionFunctions.contains(functionMetadata.name())) {
                List<JexlNode> arguments = functionMetadata.args();
                if (arguments.size() == 1) {
                    return RefactoredJexlArgumentDescriptor.Fields.product(arguments.get(0));
                }
            }
            
            return Collections.emptySet();
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
    }
    
    @Override
    public RefactoredJexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        try {
            Class<?> clazz = GetFunctionClass.get(node);
            if (!EvaluationPhaseFilterFunctions.class.equals(clazz)) {
                throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with node for a function in " + clazz);
            }
            return new EvaluationPhaseFilterJexlArgumentDescriptor(node);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
}
