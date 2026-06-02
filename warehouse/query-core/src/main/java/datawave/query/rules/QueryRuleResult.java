package datawave.query.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class QueryRuleResult {

    private final String ruleName;
    private final List<String> messages = new ArrayList<>();
    private Exception exception;

    public static QueryRuleResult of(String ruleName, String... messages) {
        QueryRuleResult result = new QueryRuleResult(ruleName);
        for (String message : messages) {
            result.addMessage(message);
        }
        return result;
    }

    public static QueryRuleResult of(String ruleName, Exception exception, String... messages) {
        QueryRuleResult result = of(ruleName, messages);
        result.setException(exception);
        return result;
    }

    public QueryRuleResult(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getRuleName() {
        return ruleName;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void addMessage(String message) {
        this.messages.add(message);
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }

    public boolean exceptionOccurred() {
        return exception != null;
    }

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

    public void addMessages(Collection<String> messages) {
        messages.forEach(this::addMessage);
    }
}
