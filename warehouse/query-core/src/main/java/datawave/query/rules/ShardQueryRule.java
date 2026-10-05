package datawave.query.rules;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * An implementation of {@link AbstractQueryRule} that is specific for use with {@link datawave.query.tables.ShardQueryLogic}. Typically, a
 * {@link ShardQueryRule} may validate either a JEXL query or a LUCENE query, but not both.
 */
public abstract class ShardQueryRule extends AbstractQueryRule {

    public ShardQueryRule() {
        super();
    }

    public ShardQueryRule(String name) {
        super(name);
    }

    public enum Syntax {
        /**
         * Indicates the supplied query is in JEXL form.
         */
        JEXL,

        /**
         * Indicates the supplied query is in LUCENE form.
         */
        LUCENE
    }

    /**
     * Returns the supported syntax for this rule.
     *
     * @return the supported syntax
     */
    protected abstract Syntax getSupportedSyntax();

    @Override
    public boolean canValidate(QueryValidationConfiguration configuration) {
        if (!(configuration instanceof ShardQueryValidationConfiguration)) {
            return false;
        }
        ShardQueryValidationConfiguration config = (ShardQueryValidationConfiguration) configuration;
        Syntax syntax = getSupportedSyntax();
        if (syntax != null) {
            Object query = config.getParsedQuery();
            // We cannot validate a null query.
            if (query == null) {
                return false;
            }
            // Determine if the query is of the supported syntax for this rule.
            switch (syntax) {
                case JEXL:
                    return query instanceof JexlNode;
                case LUCENE:
                    return query instanceof QueryNode;
                default:
                    throw new IllegalArgumentException("Cannot determine support for syntax " + syntax);
            }
        }
        return true;
    }
}
