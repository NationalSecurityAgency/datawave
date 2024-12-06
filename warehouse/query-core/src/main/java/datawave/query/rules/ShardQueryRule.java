package datawave.query.rules;

import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

public abstract class ShardQueryRule extends AbstractQueryRule {

    public ShardQueryRule() {
        super();
    }

    public ShardQueryRule(String name) {
        super(name);
    }

    protected enum Syntax {
        JEXL, LUCENE
    }

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
            if (query == null) {
                return false;
            }
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
