package datawave.query.language.processor.lucene;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.nodes.PrefixWildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.processors.WildcardQueryNodeProcessor;

/**
 * This class is intended to retain backward-compatible wildcard parsing behavior for DataWave.
 * <p>
 * That is, we're overriding {@link WildcardQueryNodeProcessor#postProcessNode(QueryNode)} here as a workaround, because newer versions of Lucene added
 * lowercase normalization and other behaviors to that method, behaviors that we want to either avoid entirely or delay/delegate instead to JEXL processing
 * further down.
 * </p>
 */
public class CustomWildcardQueryNodeProcessor extends WildcardQueryNodeProcessor {

    @Override
    protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

        // the old Lucene Parser ignores FuzzyQueryNode that are also PrefixWildcardQueryNode or WildcardQueryNode
        // we do the same here, also ignore empty terms
        if (node instanceof FieldQueryNode || node instanceof FuzzyQueryNode) {
            FieldQueryNode fqn = (FieldQueryNode) node;
            CharSequence text = fqn.getText();

            // do not process wildcards for TermRangeQueryNode children and
            // QuotedFieldQueryNode to reproduce the old parser behavior
            if (fqn.getParent() instanceof TermRangeQueryNode || fqn instanceof QuotedFieldQueryNode || text.length() <= 0) {
                // Ignore empty terms
                return node;
            }

            // Code below simulates the old lucene parser behavior for wildcards

            if (isWildcard(text)) {
                if (isPrefixWildcard(text)) {
                    return new PrefixWildcardQueryNode(fqn.getField(), text, fqn.getBegin(), fqn.getEnd());
                } else {
                    return new WildcardQueryNode(fqn.getField(), text, fqn.getBegin(), fqn.getEnd());
                }
            }

        }
        return node;
    }

    private boolean isWildcard(CharSequence text) {
        if (text == null || text.length() <= 0)
            return false;

        // If a un-escaped '*' or '?' if found return true
        // start at the end since it's more common to put wildcards at the end
        for (int i = text.length() - 1; i >= 0; i--) {
            if ((text.charAt(i) == '*' || text.charAt(i) == '?') && !UnescapedCharSequence.wasEscaped(text, i)) {
                return true;
            }
        }

        return false;
    }

    private boolean isPrefixWildcard(CharSequence text) {
        if (text == null || text.length() <= 0 || !isWildcard(text))
            return false;

        // Validate last character is a '*' and was not escaped
        // If single '*' is is a wildcard not prefix to simulate old queryparser
        if (text.charAt(text.length() - 1) != '*')
            return false;
        if (UnescapedCharSequence.wasEscaped(text, text.length() - 1))
            return false;
        if (text.length() == 1)
            return false;

        // Only make a prefix if there is only one single star at the end and no '?' or '*' characters
        // If single wildcard return false to mimic old queryparser
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '?')
                return false;
            if (text.charAt(i) == '*' && !UnescapedCharSequence.wasEscaped(text, i)) {
                if (i == text.length() - 1)
                    return true;
                else
                    return false;
            }
        }

        return false;
    }
}
