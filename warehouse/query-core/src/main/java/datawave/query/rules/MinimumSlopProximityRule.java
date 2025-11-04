package datawave.query.rules;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;

import datawave.query.lucene.visitors.InvalidSlopProximityVisitor;
import datawave.query.lucene.visitors.LuceneQueryStringBuildingVisitor;

/**
 * A {@link QueryRule} implementation that will check for any slop phrases where the number is smaller than the number of terms, e.g.
 * {@code FIELD:\"term1 term2 term3\"~1} where the 1 should be 3 or greater.
 */
public class MinimumSlopProximityRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(MinimumSlopProximityRule.class);

    public MinimumSlopProximityRule() {}

    public MinimumSlopProximityRule(String name) {
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
            // Check the query for slop phrases with invalid proximity numbers.
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<InvalidSlopProximityVisitor.InvalidSlop> invalidSlops = InvalidSlopProximityVisitor.check(luceneQuery);
            invalidSlops.stream().map(this::formatMessage).forEach(result::addMessage);
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    private String formatMessage(InvalidSlopProximityVisitor.InvalidSlop invalidSlop) {
        SlopQueryNode node = invalidSlop.getNode();
        // @formatter:off
        return new StringBuilder().append("Invalid slop proximity, the ")
                        .append(node.getValue())
                        .append(" should be ")
                        .append(invalidSlop.getMinimum())
                        .append(" or greater: ")
                        .append(LuceneQueryStringBuildingVisitor.build(node))
                        .toString();
        // @formatter:on
    }

    @Override
    public QueryRule copy() {
        return new MinimumSlopProximityRule(name);
    }
}
