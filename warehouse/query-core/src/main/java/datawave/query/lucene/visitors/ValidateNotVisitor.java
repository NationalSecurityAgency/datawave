package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
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
                    /*
                     * Case: No GroupQueryNode when a BooleanQueryNode is a child of NotBooleanQueryNode.
                     * Example: FIELD1:abc AND FIELD2:def NOT FIELD3:123 ->
                     *          FIELD1:abc AND (FIELD2:def NOT FIELD3:123)
                     */
                    throw new IllegalArgumentException("NOT junction's terms are ambiguous. Try adding parentheses around its terms.");
                }
                case MODIFIER: {

                    /*
                     * Case: Modifier(MOD_NOT) has BooleanQueryNode as child. Should have GroupQueryNode wrapping the BooleanQueryNode.
                     * Example: FIELD1:abc NOT FIELD2:def AND FIELD3:123 ->
                     *          FIELD1:123 NOT (FIELD2:456 AND FIELD3:abc)
                     */
                    if(((ModifierQueryNode)child).getModifier() == ModifierQueryNode.Modifier.MOD_NOT) {
                        for (QueryNode grandchild : child.getChildren()) {
                            QueryNodeType grandchildType = QueryNodeType.get(grandchild.getClass());
                            switch (grandchildType) {
                                case OR:
                                case AND: {
                                    throw new IllegalArgumentException("NOT junction's terms are ambiguous. Try adding parentheses around its terms.");
                                }
                            }
                        }
                    }

                    /*
                     * Case: NotBooleanQueryNode has 2 or more ModifierQueryNodes as children.
                     * Example: FIELD1:123 NOT FIELD2:456 NOT FIELD3:abc ->
                     *          FIELD1:123 NOT (FIELD2:456 NOT FIELD3:abc)
                     */
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
