package datawave.query.lucene.visitors;

import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public class ValidateQuotedFieldVisitor extends BaseVisitor {

    @Override
    public Object visit(QuotedFieldQueryNode node, Object data) {
        if (node.getFieldAsString().isEmpty() && node.getParent() != null) {
            throw new IllegalArgumentException("Quoted field cannot be empty with parent. Double check that your parentheses and quotes are in the right spots.");
        }
        visitChildren(node, data);
        return data;
    }
}
