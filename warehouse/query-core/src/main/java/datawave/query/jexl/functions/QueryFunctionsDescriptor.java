package datawave.query.jexl.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

public class QueryFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
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
            // return the true node if unable to parse arguments.
            JexlNode returnNode = TRUE_NODE;
            
            if (name.equals("between")) {
                
                JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), args.get(1).image);
                JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), args.get(2).image);
                
                // now link em up
                
                returnNode = BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode)));
            } else if (name.equals("length")) {
                // create a regex node with the appropriate number of matching characters
                
                returnNode = JexlNodeFactory.buildNode(new ASTERNode(ParserTreeConstants.JJTERNODE), args.get(0), ".{" + args.get(1).image + ','
                                + args.get(2).image + '}');
            }
            return returnNode;
        }
        
        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }
        
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // all functions use the fields in the first argument for normalization
            if (arg > 0) {
                return fields(helper, datatypeFilter);
            }
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            return JexlASTHelper.getIdentifierNames(args.get(0));
        }
        
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            return JexlArgumentDescriptor.Fields.product(args.get(0));
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
        FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
        fvis.visit(node, null);
        
        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(fvis.namespace());
        
        if (!QueryFunctions.QUERY_FUNCTION_NAMESPACE.equals(node.jjtGetChild(0).image))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of "
                            + node.jjtGetChild(0).image);
        if (!functionClass.equals(QueryFunctions.class))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in "
                            + functionClass);
        
        verify(fvis.name(), fvis.args().size());
        
        return new QueryJexlArgumentDescriptor(node, fvis.namespace(), fvis.name(), fvis.args());
    }
    
    private static void verify(String name, int numArgs) {
        if (name.equals("between")) {
            if (numArgs != 3) {
                throw new IllegalArgumentException("Wrong number of arguments to between function");
            }
        } else if (name.equals("length")) {
            if (numArgs != 3) {
                throw new IllegalArgumentException("Wrong number of arguments to length function");
            }
        } else if (name.equals(QueryFunctions.OPTIONS_FUNCTION)) {
            if (numArgs % 2 != 0) {
                throw new IllegalArgumentException("Expected even number of arguments to options function");
            }
        } else if (name.equals(QueryFunctions.UNIQUE_FUNCTION) || name.equals(QueryFunctions.GROUPBY_FUNCTION)) {
            if (numArgs == 0) {
                throw new IllegalArgumentException("Expected at least one argument to the " + name + " function");
            }
        } else {
            throw new IllegalArgumentException("Unknown Query function: " + name);
        }
    }
}
