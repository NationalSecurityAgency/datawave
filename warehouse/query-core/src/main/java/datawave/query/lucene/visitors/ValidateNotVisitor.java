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
            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch (type) {
                case OR:
                case AND: {
                    throw new IllegalArgumentException("NotBooleanQueryNode cannot have junction as claus. Add parens");
                }
                case MODIFIER: {
                    if(modSeen){
                        throw new IllegalArgumentException("NotBooleanQueryNode cannot have multiple modifiers");
                    }
                    modSeen = true;
                    break;
                }
            }
        }

        return null;
    }
}
