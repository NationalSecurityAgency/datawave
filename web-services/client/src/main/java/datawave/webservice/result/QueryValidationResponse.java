package datawave.webservice.result;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.query.exception.QueryExceptionType;

@XmlRootElement(name = "QueryValidationResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryValidationResponse extends BaseResponse {

    private static final long serialVersionUID = 1L;

    @XmlElement(name = "LogicName")
    private String logicName;

    @XmlElement(name = "QueryId")
    private String queryId;

    @XmlElementWrapper(name = "Results")
    @XmlElement(name = "Result")
    private List<Result> results;

    @XmlElementWrapper(name = "ExecutedRules")
    private List<String> executedRules;

    public String getLogicName() {
        return logicName;
    }

    public void setLogicName(String logicName) {
        this.logicName = logicName;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public List<Result> getResults() {
        return results;
    }

    public void setResults(List<Result> results) {
        this.results = results;
    }

    public List<String> getExecutedRules() {
        return executedRules;
    }

    public void setExecutedRules(List<String> executedRules) {
        this.executedRules = executedRules;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        QueryValidationResponse response = (QueryValidationResponse) object;
        return Objects.equals(logicName, response.logicName) && Objects.equals(queryId, response.queryId) && Objects.equals(results, response.results)
                        && Objects.equals(executedRules, response.executedRules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicName, queryId, results, executedRules);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryValidationResponse.class.getSimpleName() + "[", "]").add("logicName='" + logicName + "'")
                        .add("queryId='" + queryId + "'").add("results=" + results).add("executedRules=" + executedRules).toString();
    }

    @XmlAccessorType(XmlAccessType.NONE)
    @XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
    public static class Result implements Serializable {

        @XmlElement(name = "RuleName")
        private String ruleName;

        @XmlElement(name = "Messages")
        private List<String> messages;

        @XmlElement(name = "Exception")
        private QueryExceptionType exception;

        public Result() {

        }

        public Result(String ruleName, List<String> messages, QueryExceptionType exception) {
            this.ruleName = ruleName;
            this.messages = messages;
            this.exception = exception;
        }

        public String getRuleName() {
            return ruleName;
        }

        public void setRuleName(String ruleName) {
            this.ruleName = ruleName;
        }

        public List<String> getMessages() {
            return messages;
        }

        public void setMessages(List<String> messages) {
            this.messages = messages;
        }

        public QueryExceptionType getException() {
            return exception;
        }

        public void setException(QueryExceptionType exception) {
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
            Result result = (Result) object;
            return Objects.equals(ruleName, result.ruleName) && Objects.equals(messages, result.messages) && Objects.equals(exception, result.exception);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ruleName, messages, exception);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Result.class.getSimpleName() + "[", "]").add("ruleName='" + ruleName + "'").add("messages=" + messages)
                            .add("exception=" + exception).toString();
        }
    }
}
