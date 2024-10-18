package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class ValidateAndVisitor extends BaseVisitor {

    @Override
    public Object visit(AndQueryNode node, Object data) {
        for(QueryNode child : node.getChildren()) {
            QueryNodeType childType = QueryNodeType.get(child.getClass());
            switch (childType) {
                case FIELD: //AND nodes cannot have empty fields as children
                    if(((FieldQueryNode)child).getFieldAsString().isEmpty()) {
                        throw new IllegalArgumentException("AND node cannot have empty field as a child. Try adding quotations or parentheses around your terms.");
                    }
                    break;
                case OR: //AND nodes cannot have junctions as children
                case AND: {
                    throw new IllegalArgumentException("AND node cannot have junction as a claus. Try wrapping it in parenthesis.");
                }
                default:
                    break;
            }
        }
        visitChildren(node, data);
        return data;
    }

}
