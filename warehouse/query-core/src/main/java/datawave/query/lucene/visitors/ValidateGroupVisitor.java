package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class ValidateGroupVisitor extends BaseVisitor {

    @Override
    public Object visit(GroupQueryNode node, Object data) {

        for (QueryNode child : node.getChildren()) {
            QueryNodeType childType = QueryNodeType.get(child.getClass());
            switch (childType) {
                case OR:
                case AND: {
                    for (QueryNode grandchild : child.getChildren()) {
                        QueryNodeType grandchildType = QueryNodeType.get(grandchild.getClass());
                        switch (grandchildType) {
                            case FIELD: {
                                String field = ((FieldQueryNode)grandchild).getFieldAsString();
                                System.out.println("Field: " + field);
                                if (field == null || field.isEmpty()) {
                                    throw new IllegalArgumentException("GroupQueryNode cannot have a fieldless term. Try adding parentheses around the terms.");
                                }
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        }
        visitChildren(node, data);
        return null;
    }
}
