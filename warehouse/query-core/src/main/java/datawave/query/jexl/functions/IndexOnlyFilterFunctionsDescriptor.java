package datawave.query.jexl.functions;

import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Map;
import java.util.Set;

public class IndexOnlyFilterFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory{
    
    public static final String includeText = "includeText";
    
    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        try {
            Class<?> clazz = GetFunctionClass.get(node);
            if (!IndexOnlyFilterFunctions.class.equals(clazz)) {
                throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with node for a function in " + clazz);
            }
            FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
            fvis.visit(node, null);
    
            return new IndexOnlyFilterJexlArgumentDescriptor(node);
            
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    public static class IndexOnlyFilterJexlArgumentDescriptor implements JexlArgumentDescriptor {
    
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
            return null;
        }
    
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper metadata, Set<String> datatypeFilter) {
            return null;
        }
    
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper metadata, Set<String> datatypeFilter, int arg) {
            return null;
        }
    
        @Override
        public boolean useOrForExpansion() {
            return false;
        }
    
        @Override
        public boolean regexArguments() {
            return false;
        }
    
        @Override
        public boolean allowIvaratorFiltering() {
            return false;
        }
    }
}
