package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class ValidateNotVisitor extends BaseVisitor{

    @Override
    public Object visit(NotBooleanQueryNode node, Object data) {

        boolean modSeen = false;
        for(QueryNode child : node.getChildren()){
            QueryNodeType childType = QueryNodeType.get(child.getClass());
            switch (childType) {
                case OR: //NotBooleanQueryNodes cannot have junctions as children
                case AND: {
                    throw new IllegalArgumentException("NotBooleanQueryNode cannot have junction as a claus. Try wrapping it in parenthesis.");
                }
                case MODIFIER: { //NotBooleanQueryNodes cannot have more than one modifier as children
                    if(modSeen){
                        throw new IllegalArgumentException("NotBooleanQueryNode cannot have multiple modifiers.");
                    }
                    modSeen = true;
                    break;
                }
            }
        }

        visitChildren(node, data);
        return data;
    }
}
