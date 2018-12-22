package datawave.query.jexl.functions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;

import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class GroupingRequiredFilterFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     * 
     *
     */
    public static class GroupingRequiredFilterJexlArgumentDescriptor implements JexlArgumentDescriptor {
        private static final ImmutableSet<String> groupingRequiredFunctions = ImmutableSet.of("atomValuesMatch", "matchesInGroup", "matchesInGroupLeft",
                        "getGroupsForMatchesInGroup");
        
        private final ASTFunctionNode node;
        
        public GroupingRequiredFilterJexlArgumentDescriptor(ASTFunctionNode node) {
            this.node = node;
        }
        
        /**
         * Returns 'true' because none of these functions should influence the index query.
         */
        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            
            // 'true' is returned to imply that there is no range lookup possible for this function
            return TRUE_NODE;
        }
        
        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            Map<String, String> fieldValues = new HashMap<>();

            if (functionMetadata.name().equals("atomValuesMatch")) {
                // special case
                Set<String> fields = new HashSet<>();
                fields.addAll(JexlASTHelper.getIdentifierNames(functionMetadata.args().get(0)));
                fields.addAll(JexlASTHelper.getIdentifierNames(functionMetadata.args().get(1)));
                for (String fieldName: fields) {
                    EventDataQueryExpressionVisitor.ExpressionFilter f = filterMap.get(fieldName);
                    if (f == null) {
                        filterMap.put(fieldName, f = new EventDataQueryExpressionVisitor.ExpressionFilter(attributeFactory, fieldName));
                    }
                    f.acceptAllValues();
                }
            } else {
                // don't include the last argument if the size is odd as that is a position arg
                for (int i = 0; i < functionMetadata.args().size() - 1; i += 2) {
                    Set<String> fields = JexlASTHelper.getIdentifierNames(functionMetadata.args().get(i));
                    JexlNode valueNode = functionMetadata.args().get(i+1);
                    for (String fieldName: fields) {
                        EventDataQueryExpressionVisitor.ExpressionFilter f = filterMap.get(fieldName);
                        if (f == null) {
                            filterMap.put(fieldName, f = new EventDataQueryExpressionVisitor.ExpressionFilter(attributeFactory, fieldName));
                        }
                        f.addFieldPattern(valueNode.image);
                    }
                }
            }
        }
        
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            if ((!functionMetadata.name().equals("atomValuesMatch")) && ((arg % 2) == 1)) {
                return JexlASTHelper.getIdentifierNames(functionMetadata.args().get(arg - 1));
            }
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            Set<String> fields = Sets.newHashSet();
            if (functionMetadata.name().equals("atomValuesMatch")) {
                fields.addAll(JexlASTHelper.getIdentifierNames(functionMetadata.args().get(0)));
                fields.addAll(JexlASTHelper.getIdentifierNames(functionMetadata.args().get(1)));
            } else {
                // don't include the last argument if the size is odd as that is a position arg
                for (int i = 0; i < functionMetadata.args().size() - 1; i += 2) {
                    fields.addAll(JexlASTHelper.getIdentifierNames(functionMetadata.args().get(i)));
                }
            }
            return fields;
        }
        
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            
            if (functionMetadata.name().equals("atomValuesMatch")) {
                return JexlArgumentDescriptor.Fields.product(functionMetadata.args().get(0), functionMetadata.args().get(1));
            } else {
                Set<Set<String>> fieldSets = JexlArgumentDescriptor.Fields.product(functionMetadata.args().get(0));
                // don't include the last argument if the size is odd as that is a position arg
                for (int i = 2; i < functionMetadata.args().size() - 1; i += 2) {
                    fieldSets = JexlArgumentDescriptor.Fields.product(fieldSets, functionMetadata.args().get(i));
                }
                return fieldSets;
            }
        }
        
        @Override
        public boolean useOrForExpansion() {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            return true;
        }
        
        @Override
        public boolean regexArguments() {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            return false;
        }
    }
    
    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        try {
            Class<?> clazz = GetFunctionClass.get(node);
            if (!GroupingRequiredFilterFunctions.class.equals(clazz)) {
                throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with node for a function in " + clazz);
            }
            return new GroupingRequiredFilterJexlArgumentDescriptor(node);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
}
