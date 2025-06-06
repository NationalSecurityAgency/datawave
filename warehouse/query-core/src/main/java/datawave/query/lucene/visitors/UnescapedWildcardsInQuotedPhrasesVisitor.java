package datawave.query.lucene.visitors;

import static datawave.query.Constants.ASTERISK_CHAR;
import static datawave.query.Constants.BACKSLASH_CHAR;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;

/**
 * A visitor that will check a LUCENE query for any unescaped wildcard characters in quoted phrases.
 */
public class UnescapedWildcardsInQuotedPhrasesVisitor extends BaseVisitor {

    /**
     * Returns a copy of all {@link QuotedFieldQueryNode} nodes in the given tree that contain an unescaped wildcard in their phrase.
     *
     * @param query
     *            the query to examine
     * @return the list of node copies
     */
    @SuppressWarnings("unchecked")
    public static List<QuotedFieldQueryNode> check(QueryNode query) {
        UnescapedWildcardsInQuotedPhrasesVisitor visitor = new UnescapedWildcardsInQuotedPhrasesVisitor();
        return (List<QuotedFieldQueryNode>) visitor.visit(query, new ArrayList<String>());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object visit(QuotedFieldQueryNode node, Object data) {
        String text = node.getTextAsString();
        if (containsUnescapedWildcard(text)) {
            ((List<QuotedFieldQueryNode>) data).add((QuotedFieldQueryNode) copy(node));
        }
        return data;
    }

    private boolean containsUnescapedWildcard(String text) {
        if (text.isEmpty()) {
            return false;
        }
        char[] chars = text.toCharArray();
        // Check whether the first character is a backslash.
        boolean isPrevBackslash = false;

        // Examine each character in the string.
        for (char currChar : chars) {
            if (currChar == BACKSLASH_CHAR) {
                // If the previous character was a blackslash, this is an escaped backslash. Reset the isPrevBacklash to false.
                // The current character is a backslash that escapes the next character.
                isPrevBackslash = !isPrevBackslash;
            } else if (currChar == ASTERISK_CHAR) {
                // This is an escaped wildcard and can be ignored. Reset isPrevBackslash to false.
                if (isPrevBackslash) {
                    isPrevBackslash = false;
                } else {
                    return true;
                }
            } else {
                isPrevBackslash = false;
            }
        }

        return false;
    }

}
