package datawave.query.rules;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.lucene.visitors.UnfieldedTermsVisitor;

/**
 * A {@link QueryRule} implementation that will check a LUCENE query for any unfielded terms.
 */
public class UnfieldedTermsRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(UnfieldedTermsRule.class);

    public UnfieldedTermsRule() {}

    public UnfieldedTermsRule(String name) {
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
            // Check the query for any phrases with unfielded terms.
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<String> terms = UnfieldedTermsVisitor.check(luceneQuery);
            // If any unfielded terms were found, add a notice about them.
            terms.forEach(term -> result.addMessage("Unfielded term " + term + " found."));
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new UnfieldedTermsRule(name);
    }
}
