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

        boolean explicitDefaultFieldTraversed = false;
        for(QueryNode child : node.getChildren()) {

            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch(type) {
                /*
                 * Ambiguous Case:  BooleanQueryNode has sibling FieldQueryNode as children, with unorganized mixed fields.
                 * Fix:             A GroupQueryNode must wrap the BooleanQueryNode, or the groups must be organized.
                 * Example:         FIELD1:1234 5678 ->
                 *                  FIELD1:(1234 5678)
                 *                  FIELD1:(1234) OR 5678
                 *
                 * Example:         abc def FIELD:123 345 ->
                 *                  abc def (FIELD:123 345)
                 */
                case FIELD: {

                    //Tracks if we've traversed a non-empty field.
                    if(Objects.requireNonNull(((FieldQueryNode) child).getField()).equals("")) {
                        if(explicitDefaultFieldTraversed){
                            throw new IllegalArgumentException("Field terms are ambiguous. Try adding parentheses around the terms.");
                        }
                    }else{
                        explicitDefaultFieldTraversed = true;
                    }
                }
            }
        }
        visitChildren(node, data);
        return null;
    }

    @Override
    public Object visit(OrQueryNode node, Object data) {

        boolean explicitDefaultFieldTraversed = false;
        for(QueryNode child : node.getChildren()) {

            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch(type) {
                /*
                 * Ambiguous Case:  BooleanQueryNode has sibling FieldQueryNode as children, with unorganized mixed fields.
                 * Fix:             A GroupQueryNode must wrap the BooleanQueryNode, or the groups must be organized.
                 * Example:         FIELD1:1234 5678 ->
                 *                  FIELD1:(1234 5678)
                 *                  FIELD1:(1234) OR 5678
                 *
                 * Example:         abc def FIELD:123 345 ->
                 *                  abc def (FIELD:123 345)
                 */
                case FIELD: {

                    //Tracks if we've traversed a non-empty field.
                    if(Objects.requireNonNull(((FieldQueryNode) child).getField()).equals("")) {
                        if(explicitDefaultFieldTraversed){
                            throw new IllegalArgumentException("Field terms are ambiguous. Try adding parentheses around the terms.");
                        }
                    }else{
                        explicitDefaultFieldTraversed = true;
                    }
                }
            }
        }
        visitChildren(node, data);
        return null;
    }

    @Override
    public Object visit(NotBooleanQueryNode node, Object data) {

        boolean explicitDefaultFieldTraversed = false;
        for(QueryNode child : node.getChildren()) {

            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch(type) {
                /*
                 * Ambiguous Case:  BooleanQueryNode has sibling FieldQueryNode as children, with unorganized mixed fields.
                 * Fix:             A GroupQueryNode must wrap the BooleanQueryNode, or the groups must be organized.
                 * Example:         FIELD1:1234 5678 ->
                 *                  FIELD1:(1234 5678)
                 *                  FIELD1:(1234) OR 5678
                 *
                 * Example:         abc def FIELD:123 345 ->
                 *                  abc def (FIELD:123 345)
                 */
                case FIELD: {

                    //Tracks if we've traversed a non-empty field.
                    if(Objects.requireNonNull(((FieldQueryNode) child).getField()).equals("")) {
                        if(explicitDefaultFieldTraversed){
                            throw new IllegalArgumentException("Field terms are ambiguous. Try adding parentheses around the terms.");
                        }
                    }else{
                        explicitDefaultFieldTraversed = true;
                    }
                }
            }
        }
        visitChildren(node, data);
        return null;
    }
}
