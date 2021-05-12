package datawave.microservice.common.storage;

import datawave.webservice.query.Query;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Date;

public class QueryStatus implements Serializable {
    public enum QUERY_STATE {
        DEFINED, CREATED, CLOSED, CANCELED
    }
    
    private QueryKey queryKey;
    private QUERY_STATE queryState = QUERY_STATE.DEFINED;
    private Query query;
    private String plan;
    private int numResultsGenerated;
    private int numResultsReturned;
    private int concurrentNextCount;
    private Date lastUpdated;
    
    public QueryStatus() {}
    
    public QueryStatus(QueryKey queryKey) {
        setQueryKey(queryKey);
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
    
    public String getPlan() {
        return plan;
    }
    
    public void setPlan(String plan) {
        this.plan = plan;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public void setQuery(Query query) {
        this.query = query;
    }
    
    public int getNumResultsGenerated() {
        return numResultsGenerated;
    }
    
    public void setNumResultsGenerated(int numResultsGenerated) {
        this.numResultsGenerated = numResultsGenerated;
    }
    
    public int getNumResultsReturned() {
        return numResultsReturned;
    }
    
    public void setNumResultsReturned(int numResultsReturned) {
        this.numResultsReturned = numResultsReturned;
    }
    
    public int getConcurrentNextCount() {
        return concurrentNextCount;
    }
    
    public void setConcurrentNextCount(int concurrentNextCount) {
        this.concurrentNextCount = concurrentNextCount;
    }
    
    public Date getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(queryKey).append(queryState).append(query).append(plan).append(numResultsReturned).append(numResultsGenerated)
                        .build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueryStatus) {
            QueryStatus other = (QueryStatus) obj;
            return new EqualsBuilder().append(queryKey, other.queryKey).append(queryState, other.queryState).append(query, other.query).append(plan, other.plan)
                            .append(numResultsGenerated, other.numResultsGenerated).append(numResultsReturned, other.numResultsReturned).build();
        }
        return false;
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("queryKey", queryKey).append("queryState", queryState).append("query", query).append("plan", plan)
                        .append("numResultsGenerated", numResultsGenerated).append("numResultsReturned", numResultsReturned).build();
    }
}
