package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class ValidateOrVisitor extends BaseVisitor {

    @Override
    public Object visit(OrQueryNode node, Object data) {
        for(QueryNode child : node.getChildren()) {
            QueryNodeType childType = QueryNodeType.get(child.getClass());
            switch (childType) {
                case FIELD: //OR nodes cannot have empty fields as children
                    if(((FieldQueryNode)child).getFieldAsString().isEmpty()) {
                        throw new IllegalArgumentException("Or node cannot have empty field as a child. Try adding quotations or parentheses around your terms.");
                    }
                    break;
                case OR: //OR nodes cannot have junctions as children
                case AND: {
                    throw new IllegalArgumentException("Or node cannot have junction as a claus. Try wrapping it in parenthesis.");
                }
                default:
                    break;
            }
        }
        visitChildren(node, data);
        return data;
    }

}
