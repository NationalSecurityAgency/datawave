package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import java.util.Objects;

public class ValidateFieldTermsVisitor extends BaseVisitor {
    public static void validate(QueryNode node) {
        ValidateFieldTermsVisitor visitor = new ValidateFieldTermsVisitor();
        visitor.visit(node, null);
    }

    @Override
    public Object visit(AndQueryNode node, Object data) {

        for(QueryNode child : node.getChildren()) {
            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch(type) {
                /*
                 * Ambiguous Case:  BooleanQueryNode has sibling FieldQueryNode as children, one or more with empty fields.
                 * Fix:             A GroupQueryNode must wrap the BooleanQueryNode.
                 * Example:         FIELD1:1234 5678 ->
                 *                  FIELD1:(1234 5678)
                 *                  FIELD1:(1234) OR 5678
                 */
                case FIELD: {
                    if (Objects.requireNonNull(((FieldQueryNode) child).getField()).equals("")) {
                        throw new IllegalArgumentException("Field terms are ambiguous. Try adding parentheses around the terms.");
                    }
                }
            }
        }
        visitChildren(node, data);
        return null;
    }

    @Override
    public Object visit(OrQueryNode node, Object data) {
        for(QueryNode child : node.getChildren()) {
            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch(type) {
                /*
                 * Ambiguous Case:  BooleanQueryNode has sibling FieldQueryNode as children, one or more with empty fields.
                 * Fix:             A GroupQueryNode must wrap the BooleanQueryNode.
                 * Example:         FIELD1:1234 5678 ->
                 *                  FIELD1:(1234 5678)
                 *                  FIELD1:(1234) OR 5678
                 */
                case FIELD: {
                    if (Objects.requireNonNull(((FieldQueryNode) child).getField()).equals("")) {
                        throw new IllegalArgumentException("Field terms are ambiguous. Try adding parentheses around the terms.");
                    }
                }
            }
        }
        visitChildren(node, data);
        return null;
    }

    @Override
    public Object visit(NotBooleanQueryNode node, Object data) {
        for(QueryNode child : node.getChildren()) {
            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch(type) {
                /*
                 * Ambiguous Case:  BooleanQueryNode has sibling FieldQueryNode as children, one or more with empty fields.
                 * Fix:             A GroupQueryNode must wrap the BooleanQueryNode.
                 * Example:         FIELD1:1234 5678 ->
                 *                  FIELD1:(1234 5678)
                 *                  FIELD1:(1234) OR 5678
                 */
                case FIELD: {
                    if (Objects.requireNonNull(((FieldQueryNode) child).getField()).equals("")) {
                        throw new IllegalArgumentException("Field terms are ambiguous. Try adding parentheses around the terms.");
                    }
                }
                case GROUP:
            }
        }
        visitChildren(node, data);
        return null;
    }
}
