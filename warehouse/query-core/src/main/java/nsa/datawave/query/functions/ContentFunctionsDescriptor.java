package nsa.datawave.query.functions;

import java.util.List;
import java.util.Set;

import nsa.datawave.query.functions.arguments.JexlArgument;
import nsa.datawave.query.functions.arguments.JexlArgumentDescriptor;
import nsa.datawave.query.functions.arguments.JexlFieldNameArgument;
import nsa.datawave.query.functions.arguments.JexlOtherArgument;
import nsa.datawave.query.functions.arguments.JexlValueArgument;
import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.util.Metadata;

import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

@Deprecated
public class ContentFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     * 
     *
     */
    public static class ContentJexlArgumentDescriptor implements JexlArgumentDescriptor {
        
        private Logger log = Logger.getLogger(ContentJexlArgumentDescriptor.class);
        private DatawaveTreeNode node = null;
        private String zone = null;
        private int termArgStart = 0;
        
        public ContentJexlArgumentDescriptor(DatawaveTreeNode node) {
            this.node = node;
            init();
        }
        
        private void init() {
            List<JexlNode> args = node.getFunctionArgs();
            if (args.size() > 0) {
                // find the first non-ASTStringLiterals starting from the end
                int index = args.size() - 1;
                for (; index >= 0; index--) {
                    if (!(args.get(index) instanceof ASTStringLiteral)) {
                        break;
                    }
                }
                
                // set the term argument start position
                termArgStart = index + 1;
                
                // ensure we have terms
                if (termArgStart == args.size()) {
                    throw new IllegalArgumentException("Missing some arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
                
                // now determine if the number of arguments left is correct, and determine if the first field is a "zone"
                if (node.getFunctionName().equals(Constants.CONTENT_WITHIN_FUNCTION_NAME)) {
                    if (index == 2) {
                        this.zone = args.get(0).image;
                    } else if (index != 1) {
                        throw new IllegalArgumentException("Incorrect arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                    }
                } else if (node.getFunctionName().equals(Constants.CONTENT_ADJACENT_FUNCTION_NAME)) {
                    if (index == 1) {
                        this.zone = args.get(0).image;
                    } else if (index != 0) {
                        throw new IllegalArgumentException("Incorrect arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                    }
                } else if (node.getFunctionName().equals(Constants.CONTENT_PHRASE_FUNCTION_NAME)) {
                    if (index == 1) {
                        this.zone = args.get(0).image;
                    } else if (index != 0) {
                        throw new IllegalArgumentException("Incorrect arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                    }
                } else {
                    throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with an unexpected name of "
                                    + node.getFunctionName());
                }
            } else {
                throw new IllegalArgumentException("Missing arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
            }
        }
        
        @Override
        public DatawaveTreeNode getIndexQuery(Metadata metadata) {
            List<JexlNode> args = node.getFunctionArgs();
            boolean negated = node.isNegated();
            DatawaveTreeNode root = new DatawaveTreeNode(ParserTreeConstants.JJTANDNODE);
            
            // if we have more than one term, and the node is negated, then we cannot produce
            // an index query equivalent as the entire set of events should be evaluated:
            // i.e. phrase(x y) normally gives is (x and y)
            // however !phrase(x y) could match against anything with an x, y or neither.
            if ((args.size() - termArgStart) > 1 && negated) {
                return node;
            }
            
            for (int index = termArgStart; index < args.size(); index++) {
                if (zone != null) {
                    if (!metadata.getTermFrequencyFields().contains(zone)) {
                        throw new IllegalArgumentException("Cannot run a content function against a field not indexed as content: " + zone);
                    }
                    DatawaveTreeNode nu = new DatawaveTreeNode(ParserTreeConstants.JJTEQNODE);
                    nu.setFieldName(args.get(0).image);
                    nu.setFieldValue(args.get(index).image);
                    nu.setFieldValueLiteralType(ASTStringLiteral.class);
                    // if negated, then we can only have one term per the check above...so ok to set on base node
                    nu.setNegated(negated);
                    root.add(nu);
                } else {
                    // demorganize
                    DatawaveTreeNode orNode = new DatawaveTreeNode(negated ? ParserTreeConstants.JJTANDNODE : ParserTreeConstants.JJTORNODE);
                    root.add(orNode);
                    if (metadata.getTermFrequencyFields().isEmpty()) {
                        throw new IllegalArgumentException("Cannot run a content function as no fields in the system are indexed as content");
                    }
                    for (String field : metadata.getTermFrequencyFields()) {
                        DatawaveTreeNode nu = new DatawaveTreeNode(ParserTreeConstants.JJTEQNODE);
                        nu.setFieldName(field);
                        nu.setFieldValue(args.get(index).image);
                        nu.setFieldValueLiteralType(ASTStringLiteral.class);
                        // if negated, then we can only have one term per the check above...so ok to set on base node
                        nu.setNegated(negated);
                        orNode.add(nu);
                    }
                }
            }
            
            // if the root only has one child then return it
            while (root.getChildCount() == 1) {
                root = (DatawaveTreeNode) (root.getChildAt(0));
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
            String[] fieldNames = null;
            if (metadata == null) {
                fieldNames = (zone == null ? new String[0] : new String[] {zone});
            } else {
                Set<String> termFrequencyFields = metadata.getTermFrequencyFields();
                fieldNames = (zone == null ? termFrequencyFields.toArray(new String[termFrequencyFields.size()]) : new String[] {zone});
            }
            int index = 0;
            
            if (nodes.size() > 0) {
                if (zone != null) {
                    // This is the field name, however it is not to be replaced with a value from the context so treat as string literal
                    args[index] = new JexlFieldNameArgument(nodes.get(index), false);
                    index++;
                }
                if (node.getFunctionName().equals(Constants.CONTENT_WITHIN_FUNCTION_NAME)) {
                    // don't touch the distance argument, so call it OTHER
                    args[index] = new JexlOtherArgument("distance", nodes.get(index), false, fieldNames);
                    index++;
                }
                // don't touch the term offset argument, so call it OTHER
                args[index] = new JexlOtherArgument("term offset map", nodes.get(index), true, fieldNames);
                index++;
                if (nodes.size() <= index) {
                    throw new IllegalArgumentException("Missing some arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
                while (index < nodes.size()) {
                    args[index] = new JexlValueArgument(nodes.get(index), fieldNames);
                    index++;
                }
            } else {
                throw new IllegalArgumentException("Missing arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
            }
            
            return args;
        }
    }
    
    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(DatawaveTreeNode node) {
        if (!node.getFunctionNamespace().equals(Constants.CONTENT_FUNCTION_NAMESPACE))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with an unexpected namespace of "
                            + node.getFunctionNamespace());
        if (!node.getFunctionClass().equals(ContentFunctions.class))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with tree node for a function in "
                            + node.getFunctionClass().getSimpleName());
        
        return new ContentJexlArgumentDescriptor(node);
    }
    
}
