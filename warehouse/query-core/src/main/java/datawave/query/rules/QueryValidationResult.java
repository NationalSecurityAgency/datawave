package datawave.query.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class QueryValidationResult {

    private List<QueryRuleResult> ruleResults = new ArrayList<>();

    private Exception exception;

    public List<QueryRuleResult> getRuleResults() {
        return ruleResults;
    }

    public void addRuleResult(QueryRuleResult result) {
        this.ruleResults.add(result);
    }

    public void addResults(Collection<QueryRuleResult> results) {
        this.ruleResults.addAll(results);
    }

    public Exception getException() {
        return exception;
    }

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
                        .add("exceptionMessage='").toString();
    }
}
