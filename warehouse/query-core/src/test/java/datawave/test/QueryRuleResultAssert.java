package datawave.test;

import java.util.List;
import java.util.Objects;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import datawave.query.rules.QueryRuleResult;

/**
 * This class provides the ability to perform a number of assertions specific to {@link QueryRuleResult} instances, and is intended to be used for testing
 * purposes.
 */
public class QueryRuleResultAssert extends AbstractAssert<QueryRuleResultAssert,QueryRuleResult> {

    /**
     * Return a new {@link QueryRuleResultAssert} that will perform assertions on the specified result.
     *
     * @param result
     *            the result
     * @return a new {@link QueryRuleResultAssert} for the result
     */
    public static QueryRuleResultAssert assertThat(QueryRuleResult result) {
        return new QueryRuleResultAssert(result);
    }

    public QueryRuleResultAssert(QueryRuleResult result) {
        super(result, QueryRuleResultAssert.class);
    }

    /**
     * Verifies that the actual result's rule name is equal to the given one.
     *
     * @param ruleName
     *            the rule name
     * @return this {@link QueryRuleResultAssert}
     */
    public QueryRuleResultAssert hasRuleName(String ruleName) {
        isNotNull();
        if (!Objects.equals(actual.getRuleName(), ruleName)) {
            failWithMessage("Expected ruleName to be %s but was %s", ruleName, actual.getRuleName());
        }
        return this;
    }

    /**
     * Verifies that the actual result's exception is null.
     *
     * @return this {@link QueryRuleResultAssert}
     */
    public QueryRuleResultAssert hasNullException() {
        isNotNull();
        if (actual.getException() != null) {
            failWithMessage("Expected exception to be null, but was %s", actual.getException());
        }
        return this;
    }

    /**
     * Verifies that the actual result's exception is equal to the given one.
     *
     * @param exception
     *            the exception
     * @return this {@link QueryRuleResultAssert}
     */
    public QueryRuleResultAssert hasException(Exception exception) {
        isNotNull();
        if (!Objects.equals(actual.getException(), exception)) {
            failWithMessage("Expected exception to be %s but was %s", exception, actual.getException());
        }
        return this;
    }

    /**
     * Verifies that the actual result's messages is equal to the given list.
     *
     * @param messages
     *            the messages
     * @return this {@link QueryRuleResultAssert}
     */
    public QueryRuleResultAssert hasMessages(List<String> messages) {
        isNotNull();
        if (!Objects.equals(actual.getMessages(), messages)) {
            failWithMessage("Expected messages to be %n  %s%n but was %n  %s%n", messages, actual.getException());
        }
        return this;
    }

    /**
     * Verifies that the actual result's messages contains exactly the elements of the given list in any order.
     *
     * @param messages
     *            the messages
     * @return this {@link QueryRuleResultAssert}
     */
    public QueryRuleResultAssert hasExactMessagesInAnyOrder(List<String> messages) {
        isNotNull();
        // @formatter:off
        Assertions.assertThat(actual.getMessages())
                        .describedAs("Check messages contains exactly in any order")
                        .containsExactlyInAnyOrderElementsOf(messages);
        // @formatter:on
        return this;
    }
}
