package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * Validates that NOT junctions are unambiguous.
 */
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
                    throw new IllegalArgumentException("NOT junction's terms are ambiguous. Try adding parentheses around its terms.");
                }
                case MODIFIER: {
                    if(modSeen){
                        throw new IllegalArgumentException("NOT junction's terms are ambiguous. Try adding parentheses around its terms.");
                    }
                    modSeen = true;
                    break;
                }
            }
        }

        visitChildren(node, data);
        return null;
    }
}
