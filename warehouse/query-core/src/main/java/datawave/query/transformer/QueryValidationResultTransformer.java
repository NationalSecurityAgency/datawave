package datawave.query.transformer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.Transformer;

import datawave.query.rules.QueryRuleResult;
import datawave.query.rules.QueryValidationResult;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.QueryValidationResponse;

/**
 * A transformer that will transform a {@link QueryValidationResult} to a {@link QueryValidationResponse}.
 */
public class QueryValidationResultTransformer implements Transformer<Object,QueryValidationResponse> {

    private static final String UNNAMED_RULE = "UNNAMED_RULE";

    @Override
    public QueryValidationResponse transform(Object input) {
        if (!(input instanceof QueryValidationResult)) {
            throw new IllegalArgumentException("Input must be an instance of " + QueryValidationResult.class);
        }

        QueryValidationResult validationResult = (QueryValidationResult) input;

        // Transform any rule result that has either a message and/or exception to a result object.
        List<QueryValidationResponse.Result> results = new ArrayList<>();
        List<String> executedRules = new ArrayList<>();
        for (QueryRuleResult ruleResult : validationResult.getRuleResults()) {
            // If a rule name was not given, use UNNAMED_RULE.
            String ruleName = ruleResult.getRuleName();
            if (ruleName == null || ruleName.isEmpty()) {
                ruleName = UNNAMED_RULE;
            }

            // Add the rule result only if it has a message or exception.
            if (ruleResult.hasMessageOrException()) {
                // Transform the exception if one is present.
                QueryExceptionType exceptionType = null;
                Exception exception = ruleResult.getException();
                if (exception != null) {
                    exceptionType = new QueryExceptionType();
                    exceptionType.setMessage(exception.getMessage());
                    Throwable throwable = exception.getCause();
                    if (throwable != null) {
                        exceptionType.setCause(throwable.getMessage());
                    }
                }

                results.add(new QueryValidationResponse.Result(ruleName, ruleResult.getMessages(), exceptionType));
            }

            // Retain a list of all executed rules.
            executedRules.add(ruleName);
        }

        QueryValidationResponse response = new QueryValidationResponse();

        // If we had any results, add them to the response.
        if (!results.isEmpty()) {
            response.setHasResults(true);
            response.setResults(results);
        }

        // Set the executed rules.
        response.setExecutedRules(executedRules);

        // If an exception was captured in the validation result, ensure it is added to the response.
        Exception exception = validationResult.getException();
        if (exception != null) {
            response.addException(validationResult.getException());
        }

        return response;
    }
}
