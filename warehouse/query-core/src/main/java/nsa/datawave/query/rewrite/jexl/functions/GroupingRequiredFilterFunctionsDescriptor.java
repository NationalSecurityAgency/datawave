package nsa.datawave.query.rewrite.jexl.functions;

import java.util.Collections;
import java.util.Set;

import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.functions.arguments.RefactoredJexlArgumentDescriptor;
import nsa.datawave.query.util.DateIndexHelper;
import nsa.datawave.query.util.MetadataHelper;

import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.JexlNode;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class GroupingRequiredFilterFunctionsDescriptor implements RefactoredJexlFunctionArgumentDescriptorFactory {
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     * 
     *
     */
    public static class GroupingRequiredFilterJexlArgumentDescriptor implements RefactoredJexlArgumentDescriptor {
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
        public JexlNode getIndexQuery(RefactoredShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper,
                        Set<String> datatypeFilter) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            
            // 'true' is returned to imply that there is no range lookup possible for this function
            return TRUE_NODE;
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
                return RefactoredJexlArgumentDescriptor.Fields.product(functionMetadata.args().get(0), functionMetadata.args().get(1));
            } else {
                Set<Set<String>> fieldSets = RefactoredJexlArgumentDescriptor.Fields.product(functionMetadata.args().get(0));
                // don't include the last argument if the size is odd as that is a position arg
                for (int i = 2; i < functionMetadata.args().size() - 1; i += 2) {
                    fieldSets = RefactoredJexlArgumentDescriptor.Fields.product(fieldSets, functionMetadata.args().get(i));
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
    public RefactoredJexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
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
