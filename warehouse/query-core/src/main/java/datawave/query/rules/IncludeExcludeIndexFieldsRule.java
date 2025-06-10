package datawave.query.rules;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.visitors.FetchFunctionFieldsVisitor;
import datawave.query.util.MetadataHelper;

/**
 * A {@link QueryRule} implementation that will check if any indexed fields are used within the functions {@code filter:includeRegex} or
 * {@code filter:excludeRegex} in a query.
 */
public class IncludeExcludeIndexFieldsRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(IncludeExcludeIndexFieldsRule.class);

    private static final Set<Pair<String,String>> functions = Collections.unmodifiableSet(Sets.newHashSet(
                    Pair.of(EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE, EvaluationPhaseFilterFunctionsDescriptor.INCLUDE_REGEX),
                    Pair.of(EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE, EvaluationPhaseFilterFunctionsDescriptor.EXCLUDE_REGEX)));

    public IncludeExcludeIndexFieldsRule() {}

    public IncludeExcludeIndexFieldsRule(String name) {
        super(name);
    }

    @Override
    protected Syntax getSupportedSyntax() {
        return Syntax.JEXL;
    }

    @Override
    public QueryRuleResult validate(QueryValidationConfiguration ruleConfiguration) throws Exception {
        ShardQueryValidationConfiguration ruleConfig = (ShardQueryValidationConfiguration) ruleConfiguration;
        if (log.isDebugEnabled()) {
            log.debug("Validating config against instance '" + getName() + "' of " + getClass() + ": " + ruleConfig);
        }

        QueryRuleResult result = new QueryRuleResult(getName());
        try {
            MetadataHelper metadataHelper = ruleConfig.getMetadataHelper();
            ASTJexlScript jexlScript = (ASTJexlScript) ruleConfig.getParsedQuery();
            // Fetch the set of fields given within any filter:includeRegex or filter:excludeRegex function calls in the query, if any.
            Set<FetchFunctionFieldsVisitor.FunctionFields> functions = FetchFunctionFieldsVisitor.fetchFields(jexlScript,
                            IncludeExcludeIndexFieldsRule.functions, metadataHelper);
            if (!functions.isEmpty()) {
                Set<String> indexedFields = metadataHelper.getIndexedFields(null);
                // Each FunctionField object represents the collection of all fields seen for either filter:includeRegex or filter:excludeRegex.
                for (FetchFunctionFieldsVisitor.FunctionFields functionFields : functions) {
                    Set<String> intersection = Sets.intersection(indexedFields, functionFields.getFields());
                    // If the function contains any index fields, add a message to the result.
                    if (!intersection.isEmpty()) {
                        result.addMessage("Indexed fields found within the function " + functionFields.getNamespace() + ":" + functionFields.getFunction()
                                        + ": " + String.join(", ", intersection));
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }

        return result;
    }

    @Override
    public QueryRule copy() {
        return new IncludeExcludeIndexFieldsRule(name);
    }
}
