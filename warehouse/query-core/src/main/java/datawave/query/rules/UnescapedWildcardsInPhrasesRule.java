package datawave.query.rules;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;

import datawave.query.lucene.visitors.BaseVisitor;
import datawave.query.lucene.visitors.UnescapedWildcardsInQuotedPhrasesVisitor;

/**
 * A {@link QueryRule} implementation that will check a LUCENE query for any unescaped wildcard characters in a quoted phrase.
 */
public class UnescapedWildcardsInPhrasesRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(UnescapedWildcardsInPhrasesRule.class);

    public UnescapedWildcardsInPhrasesRule() {}

    public UnescapedWildcardsInPhrasesRule(String name) {
        super(name);
    }

    @Override
    protected Syntax getSupportedSyntax() {
        return Syntax.LUCENE;
    }

    @Override
    public QueryRuleResult validate(QueryValidationConfiguration configuration) throws Exception {
        ShardQueryValidationConfiguration config = (ShardQueryValidationConfiguration) configuration;
        if (log.isDebugEnabled()) {
            log.debug("Validating config against instance '" + getName() + "' of " + getClass() + ": " + config);
        }

        QueryRuleResult result = new QueryRuleResult(getName());
        try {
            // Check the query for any phrases with unescaped wildcards.
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<QuotedFieldQueryNode> nodes = UnescapedWildcardsInQuotedPhrasesVisitor.check(luceneQuery);
            // If any phrases with unescaped wildcards were found, add a notice about them.
            nodes.stream().map(this::getFormattedMessage).forEach(result::addMessage);
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new UnescapedWildcardsInPhrasesRule(name);
    }

    // Return a formatted message for the given node.
    private String getFormattedMessage(QuotedFieldQueryNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append("Unescaped wildcard found in phrase ");
        sb.append(formatQueryNode(node, true));
        sb.append(". Wildcard is incorrect, or phrase should be ");
        // Make a copy of the node with the suggested phrase format.
        QuotedFieldQueryNode copy = (QuotedFieldQueryNode) BaseVisitor.copy(node);
        copy.setText("/" + copy.getTextAsString() + "/");
        sb.append(formatQueryNode(copy, false));
        return sb.toString();
    }

    // Return a string representation of the given node.
    private String formatQueryNode(QuotedFieldQueryNode node, boolean quoted) {
        StringBuilder sb = new StringBuilder();
        String fieldName = node.getFieldAsString();
        if (!fieldName.isEmpty()) {
            sb.append(fieldName).append(":");
        }
        // If quoted is true, add quotes around the phrase.
        if (quoted) {
            sb.append("\"");
        }
        sb.append(node.getTextAsString());
        if (quoted) {
            sb.append("\"");
        }
        return sb.toString();
    }

}
