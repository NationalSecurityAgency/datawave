package datawave.query.rules;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.lucene.visitors.InvalidQuoteVisitor;
import datawave.query.lucene.visitors.LuceneQueryStringBuildingVisitor;

/**
 * A {@link QueryRule} implementation that will check a LUCENE query for any instances of ` instead of ' being used to quote a phrase.
 */
public class InvalidQuoteRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(InvalidQuoteRule.class);

    public InvalidQuoteRule() {}

    public InvalidQuoteRule(String name) {
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
            // Check the query for any phrases with invalid quotes.
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<QueryNode> phrases = InvalidQuoteVisitor.check(luceneQuery);
            // If any phrases with invalid quotes were found, add a notice about them.
            phrases.stream().map(this::formatMessage).forEach(result::addMessage);
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new InvalidQuoteRule(name);
    }

    private String formatMessage(QueryNode node) {
        // @formatter:off
        return new StringBuilder("Invalid quote ")
                        .append(InvalidQuoteVisitor.INVALID_QUOTE)
                        .append(" used in phrase \"")
                        .append(LuceneQueryStringBuildingVisitor.build(node))
                        .append("\". Use ' instead.")
                        .toString();
        // @formatter:on
    }
}
