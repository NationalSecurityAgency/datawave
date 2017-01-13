package nsa.datawave.query.language.parser.jexl;

import java.util.ArrayList;
import java.util.List;

import nsa.datawave.query.language.functions.jexl.JexlQueryFunction;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class JexlFunctionNode extends JexlNode {
    private JexlQueryFunction function = null;
    
    private JexlFunctionNode() {
        super(new ArrayList<JexlNode>());
    }
    
    public JexlFunctionNode(JexlQueryFunction function, List<String> parameterList, int depth, QueryNode parent) {
        super(new ArrayList<JexlNode>());
        
        this.function = function;
        this.function.initialize(parameterList, depth, parent);
    }
    
    public String toString() {
        if (this.function == null) {
            return "";
        } else {
            return this.function.toString();
        }
    }
}
