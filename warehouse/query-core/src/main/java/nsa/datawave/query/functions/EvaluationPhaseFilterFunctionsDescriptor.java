package nsa.datawave.query.functions;

import java.util.List;

import nsa.datawave.query.functions.arguments.JexlArgument;
import nsa.datawave.query.functions.arguments.JexlArgumentDescriptor;
import nsa.datawave.query.functions.arguments.JexlFieldNameArgument;
import nsa.datawave.query.functions.arguments.JexlOtherArgument;
import nsa.datawave.query.functions.arguments.JexlRegexArgument;
import nsa.datawave.query.functions.arguments.JexlValueArgument;
import nsa.datawave.query.parser.DatawaveTreeNode;
import nsa.datawave.query.util.Metadata;

import org.apache.commons.jexl2.parser.JexlNode;

@Deprecated
public class EvaluationPhaseFilterFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    
    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     * 
     *
     */
    public static class EvaluationPhaseFilterJexlArgumentDescriptor implements JexlArgumentDescriptor {
        
        private DatawaveTreeNode node = null;
        
        public EvaluationPhaseFilterJexlArgumentDescriptor(DatawaveTreeNode node) {
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
            // to force the function to be applied in the event evaluation phase, simply do not translate it
            // into an index query equivalent. Need to return the original function node to enable property
            // "optimized" query detection later.
            return this.node;
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
            if (node.getFunctionName().equals("isNull") || node.getFunctionName().equals("isNotNull")) {
                if (nodes.size() == 1) {
                    args[0] = new JexlFieldNameArgument(nodes.get(0));
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            } else if (node.getFunctionName().equals("includeRegex")) {
                if (nodes.size() == 2) {
                    args[0] = new JexlFieldNameArgument(nodes.get(0));
                    args[1] = new JexlRegexArgument(nodes.get(1), nodes.get(0));
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            } else if (node.getFunctionName().equals("afterLoadDate")) {
                if (nodes.size() >= 2 && nodes.size() <= 3) {
                    int i = 0;
                    args[i] = new JexlFieldNameArgument(nodes.get(i));
                    i++;
                    args[i] = new JexlValueArgument("start date", nodes.get(i), nodes.get(0));
                    i++;
                    if (nodes.size() == 3) {
                        args[i] = new JexlOtherArgument("range date format", nodes.get(i), false, nodes.get(0));
                        i++;
                    }
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            } else if (node.getFunctionName().equals("beforeLoadDate")) {
                if (nodes.size() >= 2 && nodes.size() <= 3) {
                    int i = 0;
                    args[i] = new JexlFieldNameArgument(nodes.get(i));
                    i++;
                    args[i] = new JexlValueArgument("end date", nodes.get(i), nodes.get(0));
                    i++;
                    if (nodes.size() == 3) {
                        args[i] = new JexlOtherArgument("range date format", nodes.get(i), false, nodes.get(0));
                        i++;
                    }
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            } else if (node.getFunctionName().equals("betweenLoadDates")) {
                if (nodes.size() >= 3 && nodes.size() <= 4) {
                    int i = 0;
                    args[i] = new JexlFieldNameArgument(nodes.get(i));
                    i++;
                    args[i] = new JexlValueArgument("start date", nodes.get(i), nodes.get(0));
                    i++;
                    args[i] = new JexlValueArgument("end date", nodes.get(i), nodes.get(0));
                    i++;
                    if (nodes.size() == 4) {
                        args[i] = new JexlOtherArgument("range date format", nodes.get(i), false, nodes.get(0));
                        i++;
                    }
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            } else if (node.getFunctionName().equals("afterDate")) {
                if (nodes.size() >= 2 && nodes.size() <= 4) {
                    int i = 0;
                    args[i] = new JexlFieldNameArgument(nodes.get(i));
                    i++;
                    if (nodes.size() == 4) {
                        args[i] = new JexlOtherArgument("date format", nodes.get(i), false, nodes.get(0));
                        i++;
                    }
                    args[i] = new JexlValueArgument("start date", nodes.get(i), nodes.get(0));
                    i++;
                    if (nodes.size() > 2) {
                        args[i] = new JexlOtherArgument("range date format", nodes.get(i), false, nodes.get(0));
                        i++;
                    }
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            } else if (node.getFunctionName().equals("beforeDate")) {
                if (nodes.size() >= 2 && nodes.size() <= 4) {
                    int i = 0;
                    args[i] = new JexlFieldNameArgument(nodes.get(i));
                    i++;
                    if (nodes.size() == 4) {
                        args[i] = new JexlOtherArgument("date format", nodes.get(i), false, nodes.get(0));
                        i++;
                    }
                    args[i] = new JexlValueArgument("end date", nodes.get(i), nodes.get(0));
                    i++;
                    if (nodes.size() > 2) {
                        args[i] = new JexlOtherArgument("range date format", nodes.get(i), false, nodes.get(0));
                        i++;
                    }
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            } else if (node.getFunctionName().equals("betweenDates")) {
                if (nodes.size() >= 3 && nodes.size() <= 5) {
                    int i = 0;
                    args[i] = new JexlFieldNameArgument(nodes.get(i));
                    i++;
                    if (nodes.size() == 5) {
                        args[i] = new JexlOtherArgument("date format", nodes.get(i), false, nodes.get(0));
                        i++;
                    }
                    args[i] = new JexlValueArgument("start date", nodes.get(i), nodes.get(0));
                    i++;
                    args[i] = new JexlValueArgument("end date", nodes.get(i), nodes.get(0));
                    i++;
                    if (nodes.size() > 3) {
                        args[i] = new JexlOtherArgument("range date format", nodes.get(i), false, nodes.get(0));
                        i++;
                    }
                } else {
                    throw new IllegalArgumentException("Wrong number of arguments to " + this.getClass().getSimpleName() + "." + node.getFunctionName());
                }
            }
            return args;
        }
    }
    
    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(DatawaveTreeNode node) {
        if (!node.getFunctionClass().equals(EvaluationPhaseFilterFunctions.class))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getArgumentDescriptor with tree node for a function in "
                            + node.getFunctionClass().getSimpleName());
        
        return new EvaluationPhaseFilterJexlArgumentDescriptor(node);
    }
    
}
