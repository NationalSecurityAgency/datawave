package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class ValidateModifierVisitor extends BaseVisitor {

    @Override
    public Object visit(ModifierQueryNode node, Object data) {
        for(QueryNode child : node.getChildren()) {
            QueryNodeType childType = QueryNodeType.get(child.getClass());
            switch (childType) {
                case OR: //ModifierQueryNodes cannot have junctions as children
                case AND: {
                    throw new IllegalArgumentException("Modifier cannot have junction as a claus. Try wrapping it in parenthesis.");
                }
            }
        }
        visitChildren(node, data);
        return data;
    }
}
