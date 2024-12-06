package datawave.query.rules;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import datawave.query.language.parser.lucene.EscapeQuerySyntaxImpl;
import datawave.query.lucene.visitors.AmbiguousUnfieldedTermsVisitor;
import datawave.query.lucene.visitors.BaseVisitor;
import datawave.query.lucene.visitors.LuceneQueryStringBuildingVisitor;

/**
 * An implementation of {@link QueryRule} that checks a LUCENE query for any fielded terms with unfielded terms directly ORed with it afterwards. For example:
 * <ul>
 * <li>{@code FOO:abc OR def} should be {@code FOO:(abc OR def)}</li>
 * <li>{@code (FOO:abc OR def)} should be {@code FOO:(abc OR def)}</li>
 * <li>{@code FOO:abc OR (def OR ghi)} should be {@code FOO:(abc OR def OR ghi)}</li>
 * </ul>
 */
public class AmbiguousOrPhrasesRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(AmbiguousOrPhrasesRule.class);
    private static final EscapeQuerySyntax escapedSyntax = new EscapeQuerySyntaxImpl();

    public AmbiguousOrPhrasesRule() {}

    public AmbiguousOrPhrasesRule(String name) {
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
            // Check the query for any ambiguous OR'd unfielded phrases.
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<QueryNode> nodes = AmbiguousUnfieldedTermsVisitor.check(luceneQuery, AmbiguousUnfieldedTermsVisitor.JUNCTION.OR);
            // Add a message for each ambiguous node.
            nodes.stream().map(this::formatMessage).forEach(result::addMessage);
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new AmbiguousOrPhrasesRule(name);
    }

    // Returns a formatted message for the node.
    private String formatMessage(QueryNode node) {
        // @formatter:off
        return new StringBuilder()
                        .append("Ambiguous unfielded terms OR'd with fielded term detected: ")
                        .append(LuceneQueryStringBuildingVisitor.build(node))
                        .append(" Recommended: ")
                        .append(CorrectFormatVisitor.format(node))
                        .toString();
        // @formatter:on
    }

    private static class CorrectFormatVisitor extends BaseVisitor {

        private static String format(QueryNode node) {
            CorrectFormatVisitor visitor = new CorrectFormatVisitor();
            return ((StringBuilder) visitor.visit(node, new StringBuilder())).append(")").toString();
        }

        @Override
        public Object visit(FieldQueryNode node, Object data) {
            String field = node.getFieldAsString();
            if (field.isEmpty()) {
                ((StringBuilder) data).append(" OR ").append(node.getTextAsString());
            } else {
                ((StringBuilder) data).append(field).append(":(").append(node.getTextAsString());
            }
            return data;
        }
    }
}
