package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * A visitor implementation that will check a query for any unfielded terms, e.g. {@code "term1 term2 FIELD:term3"}.
 */
public class UnfieldedTermsVisitor extends BaseVisitor {

    /**
     * Return any unfielded terms found within the query.
     *
     * @param query
     *            the query
     * @return the list of unfielded terms
     */
    @SuppressWarnings("unchecked")
    public static List<String> check(QueryNode query) {
        UnfieldedTermsVisitor visitor = new UnfieldedTermsVisitor();
        return (List<String>) visitor.visit(query, new ArrayList<String>());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object visit(FieldQueryNode node, Object data) {
        // Check if the term has a field.
        if (node.getFieldAsString().isEmpty()) {
            ((List<String>) data).add(node.getTextAsString());
        }
        return data;
    }
}
