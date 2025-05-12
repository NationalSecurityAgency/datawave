package datawave.query.rules;

import datawave.query.lucene.visitors.AmbiguousGroupedUnfieldedTermsVisitor;
import datawave.query.lucene.visitors.AmbiguousUnfieldedTermsVisitor;
import datawave.query.lucene.visitors.BaseVisitor;
import datawave.query.lucene.visitors.LuceneQueryStringBuildingVisitor;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import java.util.List;

/**
 * An implementation of {@link QueryRule} that checks a LUCENE query for any unquoted grouped phrases that are implicitly ANDED with a preceding fielded terms, e.g.
 * {@code FOO:(term1 term2 term3)} should be {@code FOO:"term1 term2 term3"}.
 */
public class AmbiguousGroupedUnquotedPhrasesRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(AmbiguousGroupedUnquotedPhrasesRule.class);

    public AmbiguousGroupedUnquotedPhrasesRule() {}

    public AmbiguousGroupedUnquotedPhrasesRule(String name) {
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
            List<QueryNode> nodes = AmbiguousGroupedUnfieldedTermsVisitor.check(luceneQuery, AmbiguousGroupedUnfieldedTermsVisitor.JUNCTION.AND);
            nodes.stream().map(this::formatMessage).forEach(result::addMessage);
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new AmbiguousGroupedUnquotedPhrasesRule(name);
    }

    // Return a message about the given nodes.
    private String formatMessage(QueryNode node) {
        // @formatter:off
        return new StringBuilder()
                        .append("Ambiguous grouped unfielded terms AND'd with fielded term detected: ")
                        .append(LuceneQueryStringBuildingVisitor.build(node))
                        .append(". Recommended: ")
                        .append(CorrectFormatVisitor.format(node))
                        .toString();
        // @formatter:on
    }

    private static class CorrectFormatVisitor extends BaseVisitor {

        private static String format(QueryNode node) {
            CorrectFormatVisitor visitor = new CorrectFormatVisitor();
            return ((StringBuilder) visitor.visit(node, new StringBuilder())).append("\"").toString();
        }

        private String prevField = "";
        @Override
        public Object visit(FieldQueryNode node, Object data) {
            String field = node.getFieldAsString();
            if (field.isEmpty() || field.contentEquals(prevField)) {
                ((StringBuilder) data).append(" ").append(node.getTextAsString());
            } else {
                prevField = field;
                ((StringBuilder) data).append(field).append(":\"").append(node.getTextAsString());
            }
            return data;
        }
    }

}
