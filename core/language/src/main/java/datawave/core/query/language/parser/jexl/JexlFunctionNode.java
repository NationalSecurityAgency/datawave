package datawave.core.query.language.parser.jexl;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.core.query.language.functions.jexl.JexlQueryFunction;

public class JexlFunctionNode extends JexlNode {
    private JexlQueryFunction function = null;

    private JexlFunctionNode() {
        super(new ArrayList<>());
    }

    public JexlFunctionNode(JexlQueryFunction function, List<String> parameterList, int depth, QueryNode parent) {
        super(new ArrayList<>());

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
