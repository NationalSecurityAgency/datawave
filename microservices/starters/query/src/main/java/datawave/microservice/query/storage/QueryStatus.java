package datawave.microservice.query.storage;

import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.CLOSE;
import static datawave.microservice.query.storage.QueryStatus.QUERY_STATE.CREATE;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryKey;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.webservice.query.exception.DatawaveErrorCode;

public class QueryStatus implements Serializable {
    private static final long serialVersionUID = -1633953472983032183L;
    
    /**
     * These are the possible query states correlating with the activity that the user is requesting.<br>
     * DEFINE: define a query<br>
     * CREATE: create a query and get results<br>
     * PLAN: plan a query<br>
     * PREDICT: predict a query<br>
     * CLOSE: close the activity (finish executing tasks in progress)<br>
     * CANCEL: cancel the activity<br>
     * FAIL: the activity failed
     */
    public enum QUERY_STATE {
        DEFINE, CREATE, PLAN, PREDICT, CLOSE, CANCEL, FAIL
    }
    
    /**
     * These are the possible stages of a query create/next/close<br>
     * CREATE: Create the query.<br>
     * PLAN: The query is being planned<br>
     * TASK: planning is complete and next tasks are being generated<br>
     * RESULTS: All next tasks have been generated and we are only generating results<br>
     */
    public enum CREATE_STAGE {
        CREATE, PLAN, TASK, RESULTS
    }
    
    private QueryKey queryKey;
    private QUERY_STATE queryState = QUERY_STATE.DEFINE;
    private CREATE_STAGE createStage = CREATE_STAGE.CREATE;
    private DatawaveUserDetails currentUser;
    private Query query;
    private GenericQueryConfiguration config;
    private Set<String> calculatedAuths;
    @JsonIgnore
    private Set<Authorizations> calculatedAuthorizations;
    private String plan;
    private Set<Prediction> predictions;
    
    private long numResultsReturned = 0L;
    private long numResultsConsumed = 0L;
    private long numResultsGenerated = 0L;
    private int activeNextCalls = 0;
    private int maxConcurrentNextCalls = 1;
    private long lastPageNumber = 0L;
    private boolean allowLongRunningQueryEmptyPages = false;
    
    private long nextCount;
    private long seekCount;
    
    // datetime of when this query started
    private long queryStartMillis;
    
    // datetime of last user interaction
    private long lastUsedMillis;
    
    // datetime of last service interaction
    private long lastUpdatedMillis;
    
    private DatawaveErrorCode errorCode;
    private String failureMessage;
    private String stackTrace;
    
    public QueryStatus() {}
    
    public QueryStatus(QueryKey queryKey) {
        setQueryKey(queryKey);
    }
    
    public boolean isProgressIdle(long currentTimeMillis, long idleTimeoutMillis) {
        // if we're processing a next request and haven't seen any activity in a while, the query is idle
        return activeNextCalls > 0 && (currentTimeMillis - lastUpdatedMillis) >= idleTimeoutMillis;
    }
    
    public boolean isUserIdle(long currentTimeMillis, long idleTimeoutMillis) {
        // if we aren't processing a next request and haven't seen any activity in a while, the query is idle
        return activeNextCalls == 0 && (currentTimeMillis - lastUsedMillis) >= idleTimeoutMillis;
    }
    
    public boolean isInactive(long currentTimeMillis, long evictionTimeoutMillis) {
        // if the query is not running and we have reached the eviction timeout, the query is inactive
        return (currentTimeMillis - Math.min(lastUsedMillis, lastUpdatedMillis)) >= evictionTimeoutMillis;
    }
    
    @JsonIgnore
    public boolean isRunning() {
        // the query is considered to be running if it is created, or closed with an open next call
        return queryState == CREATE || (queryState == CLOSE && activeNextCalls > 0);
    }
    
    public void setQueryKey(QueryKey key) {
        this.queryKey = key;
    }
    
    public QueryKey getQueryKey() {
        return queryKey;
    }
    
    public QUERY_STATE getQueryState() {
        return queryState;
    }
    
    public void setQueryState(QUERY_STATE queryState) {
        this.queryState = queryState;
    }
    
    public CREATE_STAGE getCreateStage() {
        return createStage;
    }
    
    public void setCreateStage(CREATE_STAGE createStage) {
        this.createStage = createStage;
    }
    
    public String getPlan() {
        return plan;
    }
    
    public void setPlan(String plan) {
        this.plan = plan;
    }
    
    public Set<Prediction> getPredictions() {
        return predictions;
    }
    
    public void setPredictions(Set<Prediction> predictions) {
        this.predictions = predictions;
    }
    
    public DatawaveUserDetails getCurrentUser() {
        return currentUser;
    }
    
    public void setCurrentUser(DatawaveUserDetails currentUser) {
        this.currentUser = currentUser;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public void setQuery(Query query) {
        this.query = query;
    }
    
    public GenericQueryConfiguration getConfig() {
        return config;
    }
    
    public void setConfig(GenericQueryConfiguration config) {
        this.config = config;
    }
    
    public Set<String> getCalculatedAuths() {
        if (calculatedAuths == null && calculatedAuthorizations != null) {
            calculatedAuths = this.calculatedAuthorizations.stream().flatMap(a -> a.getAuthorizations().stream())
                            .map(b -> new String(b, StandardCharsets.UTF_8)).collect(Collectors.toSet());
        }
        return calculatedAuths;
    }
    
    public void setCalculatedAuths(Set<String> calculatedAuths) {
        this.calculatedAuths = calculatedAuths;
        this.calculatedAuthorizations = null;
        getCalculatedAuthorizations();
    }
    
    public Set<Authorizations> getCalculatedAuthorizations() {
        if (calculatedAuthorizations == null && calculatedAuths != null) {
            calculatedAuthorizations = Collections.singleton(
                            new Authorizations(this.calculatedAuths.stream().map(a -> a.getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList())));
        }
        return calculatedAuthorizations;
    }
    
    public void setCalculatedAuthorizations(Set<Authorizations> calculatedAuthorizations) {
        this.calculatedAuthorizations = calculatedAuthorizations;
        this.calculatedAuths = null;
        getCalculatedAuths();
    }
    
    public DatawaveErrorCode getErrorCode() {
        return errorCode;
    }
    
    public void setErrorCode(DatawaveErrorCode errorCode) {
        this.errorCode = errorCode;
    }
    
    public String getFailureMessage() {
        return failureMessage;
    }
    
    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }
    
    public String getStackTrace() {
        return stackTrace;
    }
    
    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }
    
    @JsonIgnore
    public void setFailure(DatawaveErrorCode errorCode, Exception failure) {
        setErrorCode(errorCode);
        setFailureMessage(failure.getMessage());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        failure.printStackTrace(writer);
        writer.close();
        setStackTrace(new String(outputStream.toByteArray(), StandardCharsets.UTF_8));
    }
    
    public long getNumResultsReturned() {
        return numResultsReturned;
    }
    
    public void setNumResultsReturned(long numResultsReturned) {
        this.numResultsReturned = numResultsReturned;
    }
    
    public void incrementNumResultsReturned(long increment) {
        this.numResultsReturned += increment;
    }
    
    public long getNumResultsConsumed() {
        return numResultsConsumed;
    }
    
    public void setNumResultsConsumed(long numResultsConsumed) {
        this.numResultsConsumed = numResultsConsumed;
    }
    
    public void incrementNumResultsConsumed(long increment) {
        this.numResultsConsumed += increment;
    }
    
    public long getNumResultsGenerated() {
        return numResultsGenerated;
    }
    
    public void setNumResultsGenerated(long numResultsGenerated) {
        this.numResultsGenerated = numResultsGenerated;
    }
    
    public void incrementNumResultsGenerated(long increment) {
        this.numResultsGenerated += increment;
    }
    
    public int getActiveNextCalls() {
        return activeNextCalls;
    }
    
    public void setActiveNextCalls(int activeNextCalls) {
        this.activeNextCalls = activeNextCalls;
    }
    
    public int getMaxConcurrentNextCalls() {
        return maxConcurrentNextCalls;
    }
    
    public void setMaxConcurrentNextCalls(int maxConcurrentNextCalls) {
        this.maxConcurrentNextCalls = maxConcurrentNextCalls;
    }
    
    public long getLastPageNumber() {
        return lastPageNumber;
    }
    
    public void setLastPageNumber(long lastPageNumber) {
        this.lastPageNumber = lastPageNumber;
    }
    
    public boolean isAllowLongRunningQueryEmptyPages() {
        return allowLongRunningQueryEmptyPages;
    }
    
    public void setAllowLongRunningQueryEmptyPages(boolean allowLongRunningQueryEmptyPages) {
        this.allowLongRunningQueryEmptyPages = allowLongRunningQueryEmptyPages;
    }
    
    public long getNextCount() {
        return nextCount;
    }
    
    public void incrementNextCount(long increment) {
        this.nextCount += increment;
    }
    
    public void setNextCount(long nextCount) {
        this.nextCount = nextCount;
    }
    
    public long getSeekCount() {
        return seekCount;
    }
    
    public void incrementSeekCount(long increment) {
        this.seekCount += increment;
    }
    
    public void setSeekCount(long seekCount) {
        this.seekCount = seekCount;
    }
    
    public long getQueryStartMillis() {
        return queryStartMillis;
    }
    
    public void setQueryStartMillis(long queryStartMillis) {
        this.queryStartMillis = queryStartMillis;
    }
    
    public long getLastUsedMillis() {
        return lastUsedMillis;
    }
    
    public void setLastUsedMillis(long lastUsedMillis) {
        this.lastUsedMillis = lastUsedMillis;
    }
    
    public long getLastUpdatedMillis() {
        return lastUpdatedMillis;
    }
    
    public void setLastUpdatedMillis(long lastUpdatedMillis) {
        this.lastUpdatedMillis = lastUpdatedMillis;
    }
    
    @Override
    public int hashCode() {
        // @formatter:off
        return new HashCodeBuilder()
                .append(queryKey)
                .append(queryState)
                .append(createStage)
                .append(query)
                .append(currentUser)
                .append(calculatedAuths)
                .append(calculatedAuthorizations)
                .append(plan)
                .append(predictions)
                .append(numResultsReturned)
                .append(numResultsGenerated)
                .append(activeNextCalls)
                .append(lastPageNumber)
                .append(lastUsedMillis)
                .append(lastUpdatedMillis)
                .append(errorCode)
                .append(failureMessage)
                .append(stackTrace)
                .build();
        // @formatter:on
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueryStatus) {
            QueryStatus other = (QueryStatus) obj;
            // @formatter:off
            return new EqualsBuilder()
                    .append(queryKey, other.queryKey)
                    .append(queryState, other.queryState)
                    .append(createStage, other.createStage)
                    .append(query, other.query)
                    .append(currentUser, other.currentUser)
                    .append(calculatedAuths, other.calculatedAuths)
                    .append(calculatedAuthorizations, other.calculatedAuthorizations)
                    .append(plan, other.plan)
                    .append(predictions, other.predictions)
                    .append(numResultsReturned, other.numResultsReturned)
                    .append(numResultsGenerated, other.numResultsGenerated)
                    .append(activeNextCalls, other.activeNextCalls)
                    .append(lastPageNumber, other.lastPageNumber)
                    .append(lastUsedMillis, other.lastUsedMillis)
                    .append(lastUpdatedMillis, other.lastUpdatedMillis)
                    .append(errorCode, other.errorCode)
                    .append(failureMessage, other.failureMessage)
                    .append(stackTrace, other.stackTrace)
                    .build();
            // @formatter:on
        }
        return false;
    }
    
    @Override
    public String toString() {
        // @formatter:off
        return new ToStringBuilder(this)
                .append("queryKey", queryKey)
                .append("queryState", queryState)
                .append("createStage", createStage)
                .append("query", query)
                .append("currentUser", currentUser)
                .append("calculatedAuths", calculatedAuths)
                .append("calculatedAuthorizations", calculatedAuthorizations)
                .append("plan", plan)
                .append("predictions", predictions)
                .append("numResultsReturned", numResultsReturned)
                .append("numResultsGenerated", numResultsGenerated)
                .append("concurrentNextCount", activeNextCalls)
                .append("lastPageNumber", lastPageNumber)
                .append("lastUsed", lastUsedMillis)
                .append("lastUpdated", lastUpdatedMillis)
                .append("errorCode", errorCode)
                .append("failureMessage", failureMessage)
                .append("stackTrace", stackTrace)
                .build();
        // @formatter:on
    }
}
