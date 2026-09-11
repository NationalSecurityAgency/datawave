package datawave.query.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Represents the aggregated query validation results of multiple {@link QueryRule} instances after a query was submitted for validation and supplied to a
 * configured set of rules.
 */
public class QueryValidationResult {

    /**
     * The validation results of all {@link QueryRule} instances that the query was provided to.
     */
    private final List<QueryRuleResult> ruleResults = new ArrayList<>();

    /**
     * A captured exception that was thrown during the overall validation effort.
     */
    private Exception exception;

    /**
     * Return the rule results, possibly empty, but never null.
     *
     * @return the rule results
     */
    public List<QueryRuleResult> getRuleResults() {
        return ruleResults;
    }

    /**
     * Add a rule result.
     *
     * @param result
     *            the result to add
     */
    public void addRuleResult(QueryRuleResult result) {
        this.ruleResults.add(result);
    }

    /**
     * Add a collection of rule results.
     *
     * @param results
     *            the results to add
     */
    public void addResults(Collection<QueryRuleResult> results) {
        this.ruleResults.addAll(results);
    }

    /**
     * Returns a captured exception that was thrown during the overall validation. If no exception was thrown during validation, null will be returned.
     *
     * @return the exception
     */
    public Exception getException() {
        return exception;
    }

    /**
     * Set an exception thrown during the overall validation effort.
     *
     * @param exception
     *            the exception
     */
    public void setException(Exception exception) {
        this.exception = exception;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        QueryValidationResult that = (QueryValidationResult) object;
        return Objects.equals(ruleResults, that.ruleResults) && Objects.equals(exception, that.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleResults, exception);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryValidationResult.class.getSimpleName() + "[", "]").add("ruleResults=" + ruleResults).add("exception=" + exception)
                        .toString();
    }
}
