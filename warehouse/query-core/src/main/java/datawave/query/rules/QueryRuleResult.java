package datawave.query.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Represents the query validation result of a single {@link QueryRule}.
 */
public class QueryRuleResult {

    /**
     * The original name of the rule.
     */
    private final String ruleName;

    /**
     * A list of messages describing potential issues with the query.
     */
    private final List<String> messages = new ArrayList<>();

    /**
     * A captured exception that was thrown during validation.
     */
    private Exception exception;

    /**
     * Return a new {@link QueryRuleResult} with the given rule name and messages.
     *
     * @param ruleName
     *            the rule name
     * @param messages
     *            the messages
     * @return the new result
     */
    public static QueryRuleResult of(String ruleName, String... messages) {
        QueryRuleResult result = new QueryRuleResult(ruleName);
        for (String message : messages) {
            result.addMessage(message);
        }
        return result;
    }

    /**
     * Return a new {@link QueryRuleResult} with the given rule name, exception, and messages.
     *
     * @param ruleName
     *            the rule name
     * @param exception
     *            the exception
     * @param messages
     *            the messages
     * @return the new result
     */
    public static QueryRuleResult of(String ruleName, Exception exception, String... messages) {
        QueryRuleResult result = of(ruleName, messages);
        result.setException(exception);
        return result;
    }

    public QueryRuleResult(String ruleName) {
        this.ruleName = ruleName;
    }

    /**
     * Return the original rule name of the {@link QueryRule} that provided this {@link QueryRuleResult}.
     *
     * @return the rule name
     */
    public String getRuleName() {
        return ruleName;
    }

    /**
     * Return the list of messages describing potential issues with the query. Possibly empty, but never null.
     *
     * @return the messages
     */
    public List<String> getMessages() {
        return messages;
    }

    /**
     * Add a message to this {@link QueryRuleResult}.
     *
     * @param message
     *            the message tot add
     */
    public void addMessage(String message) {
        if (message != null) {
            this.messages.add(message);
        }
    }

    /**
     * Add a collection of messages to this {@link QueryRuleResult}.
     *
     * @param messages
     *            the messages to add
     */
    public void addMessages(Collection<String> messages) {
        messages.forEach(this::addMessage);
    }

    /**
     * Set an exception thrown during validation.
     *
     * @param exception
     *            the exception
     */
    public void setException(Exception exception) {
        this.exception = exception;
    }

    /**
     * Returns a captured exception that was thrown during validation. If no exception was thrown during validation, null will be returned.
     *
     * @return the exception
     */
    public Exception getException() {
        return exception;
    }

    /**
     * Returns whether an exception was thrown and captured during validation.
     *
     * @return true if an exception was thrown, or false otherwise
     */
    public boolean exceptionOccurred() {
        return exception != null;
    }

    /**
     * Returns whether an exception was thrown and captured during validation, or any messages were added to this {@link QueryRuleResult}.
     *
     * @return true if an exception was thrown or any messages were added, or false otherwise
     */
    public boolean hasMessageOrException() {
        return exceptionOccurred() || !messages.isEmpty();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        QueryRuleResult that = (QueryRuleResult) object;
        return Objects.equals(ruleName, that.ruleName) && Objects.equals(messages, that.messages) && Objects.equals(exception, that.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleName, messages, exception);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryRuleResult.class.getSimpleName() + "[", "]").add("ruleName='" + ruleName + "'").add("messages=" + messages)
                        .add("exception=" + exception).toString();
    }
}
