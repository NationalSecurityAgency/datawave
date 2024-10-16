package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class ValidateNotVisitor extends BaseVisitor{

    public static void validate(QueryNode node) {
        ValidateNotVisitor visitor = new ValidateNotVisitor();
        visitor.visit(node, null);
    }

    @Override
    public Object visit(NotBooleanQueryNode node, Object data) {

        boolean modSeen = false;
        for(QueryNode child : node.getChildren()){
            QueryNodeType childType = QueryNodeType.get(child.getClass());
            switch (childType) {
                case OR:
                case AND: {
                    throw new IllegalArgumentException("NotBooleanQueryNode cannot have junction as a claus. Try wrapping it in parenthesis.");
                }
                case MODIFIER: {
                    if(modSeen){
                        throw new IllegalArgumentException("NotBooleanQueryNode cannot have multiple modifiers.");
                    }
                    modSeen = true;

                    for(QueryNode grandchild : child.getChildren()) {
                        QueryNodeType grandchildType = QueryNodeType.get(grandchild.getClass());
                        switch (grandchildType) {
                            case OR:
                            case AND: {
                                throw new IllegalArgumentException("Modifier cannot have junction as a claus. Try wrapping it in parenthesis.");
                            }
                        }
                    }
                    break;
                }
            }
        }

        return null;
    }
}
