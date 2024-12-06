package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FunctionQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * A visitor that will check a query for any quoted phrases that have the quote character {@value #INVALID_QUOTE} instead of {@code '}.
 */
public class InvalidQuoteVisitor extends BaseVisitor {

    public static final char INVALID_QUOTE = '`';

    /**
     * Returns a list of copies of any phrases in the query that uses the quote character {@value #INVALID_QUOTE} at either end instead of {@code '}. This also
     * applies to any parameters in functions.
     *
     * @param query
     *            the query to examine
     * @return a list of copies of phrases with invalid quotes
     */
    public static List<QueryNode> check(QueryNode query) {
        InvalidQuoteVisitor visitor = new InvalidQuoteVisitor();
        // noinspection unchecked
        return (List<QueryNode>) visitor.visit(query, new ArrayList<QueryNode>());
    }

    @Override
    public Object visit(FieldQueryNode node, Object data) {
        String text = node.getTextAsString();
        // Check if the string either starts with or ends with the invalid quote character.
        if (containsInvalidQuote(text)) {
            // noinspection unchecked
            ((List<QueryNode>) data).add(copy(node));
        }
        return data;
    }

    @Override
    public Object visit(FunctionQueryNode node, Object data) {
        // Check if any of the function arguments have invalid quotes.
        for (String arg : node.getParameterList()) {
            if (containsInvalidQuote(arg)) {
                // noinspection unchecked
                ((List<QueryNode>) data).add(copy(node));
                break;
            }
        }
        return data;
    }

    private boolean containsInvalidQuote(String text) {
        return !text.isEmpty() && (text.charAt(0) == INVALID_QUOTE || (text.charAt(text.length() - 1) == INVALID_QUOTE));
    }

}
