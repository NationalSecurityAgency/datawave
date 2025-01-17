package datawave.query.transformer;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.query.rules.QueryRuleResult;
import datawave.query.rules.QueryValidationResult;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.QueryValidationResponse;

class QueryValidationResultTransformerTest {

    private final QueryValidationResultTransformer transformer = new QueryValidationResultTransformer();

    private QueryValidationResult result;

    private QueryValidationResponse expectedResponse;

    @AfterEach
    void tearDown() {
        result = null;
        expectedResponse = null;
    }

    /**
     * Test an empty query validation result.
     */
    @Test
    void testEmptyResult() {
        givenResult(new QueryValidationResult());

        QueryValidationResponse response = new QueryValidationResponse();
        response.setExecutedRules(List.of());
        expectResponse(response);

        assertResult();
    }

    /**
     * Test a validation result that only has an exception.
     */
    @Test
    void testResultWithExceptionOnly() {
        QueryValidationResult result = new QueryValidationResult();
        IllegalArgumentException exception = new IllegalArgumentException("I failed!");
        result.setException(exception);
        givenResult(result);

        QueryValidationResponse response = new QueryValidationResponse();
        response.addException(exception);
        response.setExecutedRules(List.of());
        expectResponse(response);

        assertResult();
    }

    /**
     * Test a validation result with query results that should be included.
     */
    @Test
    void testResultWithRuleResultsOnly() {
        IllegalArgumentException exception = new IllegalArgumentException("I failed!");

        QueryValidationResult result = new QueryValidationResult();
        result.addRuleResult(QueryRuleResult.of("", "I am unnamed"));
        result.addRuleResult(QueryRuleResult.of("Rule 1", "I have a message", "I have two messages"));
        result.addRuleResult(QueryRuleResult.of("Rule 2 with no message"));
        result.addRuleResult(QueryRuleResult.of("Rule 3 with exception", exception));
        givenResult(result);

        List<QueryValidationResponse.Result> responseList = new ArrayList<>();
        responseList.add(new QueryValidationResponse.Result("UNNAMED_RULE", List.of("I am unnamed"), null));
        responseList.add(new QueryValidationResponse.Result("Rule 1", List.of("I have a message", "I have two messages"), null));
        responseList.add(new QueryValidationResponse.Result("Rule 3 with exception", List.of(), new QueryExceptionType("I failed!", null, null)));
        QueryValidationResponse response = new QueryValidationResponse();
        response.setResults(responseList);
        response.setExecutedRules(List.of("UNNAMED_RULE", "Rule 1", "Rule 2 with no message", "Rule 3 with exception"));
        expectResponse(response);

        assertResult();
    }

    /**
     * Test a validation result with query results and an exception.
     */
    @Test
    void testResultWithRuleResultsAndException() {
        IllegalArgumentException exception = new IllegalArgumentException("I failed!");
        IllegalArgumentException secondException = new IllegalArgumentException("I failed big time.");

        QueryValidationResult result = new QueryValidationResult();
        result.addRuleResult(QueryRuleResult.of("", "I am unnamed"));
        result.addRuleResult(QueryRuleResult.of("Rule 1", "I have a message", "I have two messages"));
        result.addRuleResult(QueryRuleResult.of("Rule 2 with no message"));
        result.addRuleResult(QueryRuleResult.of("Rule 3 with exception", exception));
        result.setException(secondException);
        givenResult(result);

        List<QueryValidationResponse.Result> responseList = new ArrayList<>();
        responseList.add(new QueryValidationResponse.Result("UNNAMED_RULE", List.of("I am unnamed"), null));
        responseList.add(new QueryValidationResponse.Result("Rule 1", List.of("I have a message", "I have two messages"), null));
        responseList.add(new QueryValidationResponse.Result("Rule 3 with exception", List.of(), new QueryExceptionType("I failed!", null, null)));
        QueryValidationResponse response = new QueryValidationResponse();
        response.setResults(responseList);
        response.setExecutedRules(List.of("UNNAMED_RULE", "Rule 1", "Rule 2 with no message", "Rule 3 with exception"));
        response.addException(secondException);
        expectResponse(response);

        assertResult();
    }

    /**
     * Test a validation result that had a query rule result with an exception with a cause.
     */
    @Test
    void testResultWithRuleResultWithExceptionWithCause() {
        IllegalArgumentException cause = new IllegalArgumentException("I failed!");
        IllegalArgumentException exception = new IllegalArgumentException("I failed big time.", cause);

        QueryValidationResult result = new QueryValidationResult();
        result.addRuleResult(QueryRuleResult.of("Rule Name", exception));
        result.setException(exception);
        givenResult(result);

        List<QueryValidationResponse.Result> responseList = new ArrayList<>();
        responseList.add(new QueryValidationResponse.Result("Rule Name", List.of(), new QueryExceptionType("I failed big time.", "I failed!", null)));
        QueryValidationResponse response = new QueryValidationResponse();
        response.setResults(responseList);
        response.setExecutedRules(List.of("Rule Name"));
        response.addException(exception);
        expectResponse(response);

        assertResult();
    }

    /**
     * Test a validation result with no relevant query results that should be included.
     */
    @Test
    void testResultWithNoRelevantRuleResults() {
        IllegalArgumentException exception = new IllegalArgumentException("I failed!");

        QueryValidationResult result = new QueryValidationResult();
        result.addRuleResult(QueryRuleResult.of(""));
        result.addRuleResult(QueryRuleResult.of("Rule 1"));
        result.addRuleResult(QueryRuleResult.of("Rule 2"));
        result.addRuleResult(QueryRuleResult.of("Rule 3"));
        givenResult(result);

        QueryValidationResponse response = new QueryValidationResponse();
        response.setExecutedRules(List.of("UNNAMED_RULE", "Rule 1", "Rule 2", "Rule 3"));
        expectResponse(response);

        assertResult();
    }

    private void givenResult(QueryValidationResult result) {
        this.result = result;
    }

    private void expectResponse(QueryValidationResponse response) {
        this.expectedResponse = response;
    }

    private void assertResult() {
        QueryValidationResponse actual = transformer.transform(result);
        Assertions.assertEquals(expectedResponse, actual);
    }
}
