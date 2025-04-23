package datawave.query.rules;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.lucene.visitors.AmbiguousNotVisitor;
import datawave.query.lucene.visitors.LuceneQueryStringBuildingVisitor;

/**
 * An implementation of {@link QueryRule} that checks a LUCENE query for any usage of NOT with OR'd/AND'd terms before it that are not wrapped, e.g. *
 * {@code FIELD1:abc OR FIELD2:def NOT FIELD3:123} should be {@code (FIELD1:abc OR FIELD2:def) NOT FIELD3:123}.
 */
public class AmbiguousNotRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(AmbiguousNotRule.class);

    public AmbiguousNotRule() {}

    public AmbiguousNotRule(String name) {
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
            // Check the query for any ambiguous usage of NOT.
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<NotBooleanQueryNode> nodes = AmbiguousNotVisitor.check(luceneQuery);
            // Add a message for each ambiguous NOT.
            nodes.stream().map(this::formatMessage).forEach(result::addMessage);
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new AmbiguousNotRule(name);
    }

    // Return a message about the given node.
    private String formatMessage(NotBooleanQueryNode node) {
        StringBuilder sb = new StringBuilder();
        sb.append("Ambiguous usage of NOT detected with multiple unwrapped preceding terms: ");

        String precedingTerms = getPrecedingTerms(node);
        // @formatter:off
        return sb.append("\"")
                        .append(precedingTerms)
                        .append(" NOT\" should be \"(")
                        .append(precedingTerms)
                        .append(") NOT\".")
                        .toString();
        // @formatter:on
    }

    // Return the terms preceding the NOT in the given node as a nicely formatted query string.
    private String getPrecedingTerms(NotBooleanQueryNode node) {
        QueryNode junctionNode = node.getChildren().get(0);
        String junction = junctionNode instanceof AndQueryNode ? " AND " : " OR ";
        // @formatter:off
        return junctionNode.getChildren().stream()
                        .map(LuceneQueryStringBuildingVisitor::build)
                        .collect(Collectors.joining(junction));
        // @formatter:on
    }

}
