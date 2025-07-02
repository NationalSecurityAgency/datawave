package datawave.query.rules;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.DateType;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.visitors.FetchFunctionFieldsVisitor;
import datawave.query.util.TypeMetadata;

/**
 * A {@link QueryRule} implementation that will check if any time-based functions are used with non-date fields in a query.
 */
public class TimeFunctionRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(TimeFunctionRule.class);

    private static final Set<Pair<String,String>> functions = Collections.unmodifiableSet(Sets
                    .newHashSet(Pair.of(EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE, EvaluationPhaseFilterFunctionsDescriptor.TIME_FUNCTION)));

    private static final String DATE_TYPE = DateType.class.getName();

    public TimeFunctionRule() {}

    public TimeFunctionRule(String name) {
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
            ASTJexlScript jexlScript = (ASTJexlScript) ruleConfig.getParsedQuery();
            // Fetch the set of fields used in all of the time functions.
            Set<FetchFunctionFieldsVisitor.FunctionFields> functions = FetchFunctionFieldsVisitor.fetchFields(jexlScript, TimeFunctionRule.functions,
                            ruleConfig.getMetadataHelper());
            // If any time functions were used in the query, check the field types.
            if (!functions.isEmpty()) {
                TypeMetadata typeMetadata = ruleConfig.getTypeMetadata();
                // A temporary cache to avoid unnecessary lookups via TypeMetadata if we see a field more than once.
                Multimap<String,String> types = HashMultimap.create();
                for (FetchFunctionFieldsVisitor.FunctionFields functionFields : functions) {
                    // Find any fields that are not a date type. Maintain insertion order.
                    Set<String> invalidFields = new LinkedHashSet<>();
                    for (String field : functionFields.getFields()) {
                        if (!types.containsKey(field)) {
                            types.putAll(field, typeMetadata.getNormalizerNamesForField(field));
                        }
                        if (!types.containsEntry(field, DATE_TYPE)) {
                            invalidFields.add(field);
                        }
                    }
                    // If any non-date type fields were found, add a message to the result.
                    if (!invalidFields.isEmpty()) {
                        result.addMessage("Function #TIME_FUNCTION (filter:timeFunction) found with fields that are not date types: "
                                        + String.join(", ", invalidFields));
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
        return new TimeFunctionRule(name);
    }
}
