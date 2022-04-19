package datawave.query.jexl.functions;

import com.google.common.collect.ImmutableSet;
import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IndexOnlyFilterFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory{
    
    public static final String EXCLUDE_REGEX = "excludeRegex";
    public static final String INCLUDE_REGEX = "includeRegex";
    public static final String IS_NULL = "isNull";
    public static final String BETWEEN_DATES = "betweenDates";
    public static final String BETWEEN_LOAD_DATES = "betweenLoadDates";
    public static final String MATCHES_AT_LEAST_COUNT_OF = "matchesAtLeastCountOf";
    public static final String TIME_FUNCTION = "timeFunction";
    public static final String INCLUDE_TEXT = "includeText";
    public static final String COMPARE = "compare";
    public static final String NO_EXPANSION = "noExpansion";
    
    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        try {
            Class<?> clazz = GetFunctionClass.get(node);
            if (!IndexOnlyFilterFunctions.class.equals(clazz)) {
                throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with node for a function in " + clazz);
            }
            FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
            visitor.visit(node, null);
    
            if (COMPARE.equalsIgnoreCase(visitor.name())) {
                CompareFunctionValidator.validate(visitor.name(), visitor.args());
            }
            
            return new IndexOnlyFilterJexlArgumentDescriptor(node);
            
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    public static class IndexOnlyFilterJexlArgumentDescriptor implements JexlArgumentDescriptor {
    
        private static final Logger log = Logger.getLogger(IndexOnlyFilterJexlArgumentDescriptor.class);
    
        public static final ImmutableSet<String> regexFunctions = ImmutableSet.of(EXCLUDE_REGEX, INCLUDE_REGEX);
        public static final ImmutableSet<String> andExpansionFunctions = ImmutableSet.of(IS_NULL);
        public static final ImmutableSet<String> dateBetweenFunctions = ImmutableSet.of(BETWEEN_DATES, BETWEEN_LOAD_DATES);
        
        private final ASTFunctionNode node;
    
        public IndexOnlyFilterJexlArgumentDescriptor(ASTFunctionNode node) {
            this.node = node;
        }
    
        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration settings, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                        Set<String> datatypeFilter) {
            return null;
        }
    
        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
        
        }
    
        @Override
        public Set<String> fields(MetadataHelper metadata, Set<String> datatypeFilter) {
            JexlNode[] fieldNodes = getFunctionFieldNodes();
            // @formatter:off
            return Arrays.stream(fieldNodes)
                            .map(JexlASTHelper::getIdentifierNames)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
            // @formatter:on
        }
    
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper metadata, Set<String> datatypeFilter) {
            JexlNode[] fieldNodes = getFunctionFieldNodes();
            return fieldNodes.length == 1 ? JexlArgumentDescriptor.Fields.product(fieldNodes[0]) : JexlArgumentDescriptor.Fields.product(fieldNodes[0], fieldNodes[1]);
        }
    
        private JexlNode[] getFunctionFieldNodes() {
            FunctionJexlNodeVisitor visitor = getFunctionVisitor();
            List<JexlNode> args = visitor.args();
            switch (visitor.name()) {
                case MATCHES_AT_LEAST_COUNT_OF:
                    return new JexlNode[] { args.get(1) };
                case TIME_FUNCTION:
                    return new JexlNode[] {args.get(0), args.get(1)};
                case COMPARE:
                    return new JexlNode[] {args.get(0), args.get(3)};
                default:
                    return new JexlNode[] {args.get(0)};
            }
        }
        
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper metadata, Set<String> datatypeFilter, int arg) {
            return Collections.emptySet();
        }
    
        @Override
        public boolean useOrForExpansion() {
            return !queryContains(andExpansionFunctions);
        }
    
        @Override
        public boolean regexArguments() {
            return queryContains(regexFunctions);
        }
    
        private boolean queryContains(Set<String> functions) {
            return functions.contains(getFunctionVisitor().name());
        }
        
        private FunctionJexlNodeVisitor getFunctionVisitor() {
            FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
            node.jjtAccept(visitor, null);
            return visitor;
        }
        
        @Override
        public boolean allowIvaratorFiltering() {
            return true;
        }
    }
}
