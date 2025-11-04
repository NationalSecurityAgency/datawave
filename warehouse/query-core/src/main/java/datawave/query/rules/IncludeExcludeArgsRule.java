package datawave.query.rules;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.lucene.visitors.InvalidIncludeExcludeArgsVisitor;

/**
 * A {@link QueryRule} implementation that validates the arguments of any {@code #INCLUDE} or {@code #EXCLUDE} functions found within a LUCENE query.
 */
public class IncludeExcludeArgsRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(IncludeExcludeArgsRule.class);

    private static final String NO_ARGS_MESSAGE = "Function #%s supplied with no arguments. Must supply at least a field and value, e.g. #%s(FIELD, 'value').";
    private static final String UNEVEN_ARGS_MESSAGE = "Function #%s supplied with uneven number of arguments. Must supply field/value pairs, e.g. "
                    + "#%s(FIELD, 'value') or #%s(FIELD1, 'value1', FIELD2, 'value2').";
    private static final String NO_ARGS_AFTER_BOOLEAN_MESSAGE = "Function #%s supplied with no arguments after the first boolean arg %s. "
                    + "Must supply at least a field and value after the first boolean arg, e.g. #%s(%s, FIELD, 'value').";
    private static final String UNEVEN_ARGS_AFTER_BOOLEAN_MESSAGE = "Function #%s supplied with uneven number of arguments after the first boolean arg %s. "
                    + "Must supply field/value after the boolean, e.g. #%s(%s, FIELD, 'value') or #%s(%s, FIELD1, 'value1',' FIELD2, 'value2').";

    public IncludeExcludeArgsRule() {}

    public IncludeExcludeArgsRule(String name) {
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
            // Check the query for any #INCLUDE or #EXCLUDE functions with invalid arguments.
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<InvalidIncludeExcludeArgsVisitor.InvalidFunction> functions = InvalidIncludeExcludeArgsVisitor.check(luceneQuery);
            functions.stream().map(this::formatMessage).forEach(result::addMessage);
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new IncludeExcludeArgsRule(name);
    }

    // Return a formatted message that is specific to the reason.
    private String formatMessage(InvalidIncludeExcludeArgsVisitor.InvalidFunction function) {
        switch (function.getReason()) {
            case NO_ARGS:
                return formatNoArgsMessage(function);
            case UNEVEN_ARGS:
                return formatUnevenArgsMessage(function);
            case NO_ARGS_AFTER_BOOLEAN:
                return formatNoArgsAfterBooleanMessage(function);
            case UNEVEN_ARGS_AFTER_BOOLEAN:
                return formatUnevenArgsAfterBooleanMessage(function);
            default:
                throw new IllegalArgumentException("No message configured for scenario " + function.getReason());
        }
    }

    // Return a formatted message for a no-args scenario.
    private String formatNoArgsMessage(InvalidIncludeExcludeArgsVisitor.InvalidFunction function) {
        String name = function.getName();
        return String.format(NO_ARGS_MESSAGE, name, name);
    }

    // Return a formatted message for an uneven args scenario.
    private String formatUnevenArgsMessage(InvalidIncludeExcludeArgsVisitor.InvalidFunction function) {
        String name = function.getName();
        return String.format(UNEVEN_ARGS_MESSAGE, name, name, name);
    }

    // Return a formatted message for a no args after a boolean scenario.
    private String formatNoArgsAfterBooleanMessage(InvalidIncludeExcludeArgsVisitor.InvalidFunction function) {
        String name = function.getName();
        String booleanArg = function.getArgs().get(0);
        return String.format(NO_ARGS_AFTER_BOOLEAN_MESSAGE, name, booleanArg, name, booleanArg);
    }

    // Returns a formatted message for an uneven args after a boolean scenario.
    private String formatUnevenArgsAfterBooleanMessage(InvalidIncludeExcludeArgsVisitor.InvalidFunction function) {
        String name = function.getName();
        String booleanArg = function.getArgs().get(0);
        return String.format(UNEVEN_ARGS_AFTER_BOOLEAN_MESSAGE, name, booleanArg, name, booleanArg, name, booleanArg);
    }
}
