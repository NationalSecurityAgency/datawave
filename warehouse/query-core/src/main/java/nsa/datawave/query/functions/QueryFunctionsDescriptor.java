package nsa.datawave.query.functions;

import java.util.List;

import nsa.datawave.query.functions.arguments.JexlArgument;
import nsa.datawave.query.functions.arguments.JexlArgumentDescriptor;
import nsa.datawave.query.functions.arguments.JexlFieldNameArgument;
import nsa.datawave.query.functions.arguments.JexlValueArgument;
import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.util.Metadata;

import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

@Deprecated
public class QueryFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     * 
     *
     */
    public static class QueryJexlArgumentDescriptor implements JexlArgumentDescriptor {
        
        private DatawaveTreeNode node = null;
        
        public QueryJexlArgumentDescriptor(DatawaveTreeNode node) {
            this.node = node;
            init();
        }
        
        private void init() {
            List<JexlNode> args = node.getFunctionArgs();
            if (args.size() == 0) {
                throw new IllegalArgumentException("Missing arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
            }
        }
        
        @Override
        public DatawaveTreeNode getIndexQuery(Metadata metadata) {
            DatawaveTreeNode root = null;
            List<JexlNode> args = node.getFunctionArgs();
            // at this point we expect the argument to be the field name
            if (node.getFunctionName().equals("between")) {
                if (args.size() == 3) {
                    root = new DatawaveTreeNode(ParserTreeConstants.JJTLENODE);
                    root.setRangeNode(true);
                    root.setFieldName(args.get(0).image);
                    root.setRangeLowerOp(">=");
                    root.setLowerBound(args.get(1).image);
                    root.setRangeUpperOp("<=");
                    root.setUpperBound(args.get(2).image);
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            } else if (node.getFunctionName().equals("length")) {
                if (args.size() == 3) {
                    root = new DatawaveTreeNode(ParserTreeConstants.JJTERNODE);
                    root.setFieldName(args.get(0).image);
                    root.setFieldValue(".*");
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            }
            if (root != null) {
                root.setNegated(node.isNegated());
            }
            return root;
        }
        
        @Override
        public JexlArgument[] getArguments() {
            return getArgumentsWithFieldNames(null);
        }
        
        @Override
        public JexlArgument[] getArgumentsWithFieldNames(Metadata metadata) {
            List<JexlNode> nodes = node.getFunctionArgs();
            JexlArgument[] args = new JexlArgument[nodes.size()];
            
            // at this point we expect the argument to be the field name
            if (node.getFunctionName().equals("between") || node.getFunctionName().equals("length")) {
                if (nodes.size() == 3) {
                    args[0] = new JexlFieldNameArgument(nodes.get(0));
                    args[1] = new JexlValueArgument("lower bound", nodes.get(1), nodes.get(0));
                    args[2] = new JexlValueArgument("upper bound", nodes.get(2), nodes.get(0));
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            } else if (node.getFunctionName().equals("abs")) {
                if (nodes.size() == 1) {
                    args[0] = new JexlFieldNameArgument(nodes.get(0));
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            }
            return args;
        }
    }
    
    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(DatawaveTreeNode node) {
        if (!node.getFunctionClass().equals(QueryFunctions.class))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with tree node for a function in "
                            + node.getFunctionClass().getSimpleName());
        
        return new QueryJexlArgumentDescriptor(node);
    }
    
}
