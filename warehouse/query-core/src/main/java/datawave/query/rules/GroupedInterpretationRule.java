package datawave.query.rules;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.lucene.visitors.LuceneQueryStringBuildingVisitor;
import datawave.query.lucene.visitors.GroupedInterpretationVisitor;

/**
 * An implementation of {@link QueryRule} that checks a LUCENE query for any grouped phrases
 */
public class GroupedInterpretationRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(GroupedInterpretationRule.class);

    public GroupedInterpretationRule() {}

    public GroupedInterpretationRule(String name) {
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
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<QueryNode> interpretNodes = GroupedInterpretationVisitor.check(luceneQuery, GroupedInterpretationVisitor.JUNCTION.AND);
            interpretNodes.stream().map(this::formatMessage).forEach(result::addMessage);
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new AmbiguousUnquotedPhrasesRule(name);
    }

    // Return a message about the given nodes.
    private String formatMessage(QueryNode node) {
        // @formatter:off
        return new StringBuilder()
                //.append(query)
                .append("Portion will be interpreted as: ")
                .append(LuceneQueryStringBuildingVisitor.build(node))
                .toString();
        // @formatter:on
    }

}
