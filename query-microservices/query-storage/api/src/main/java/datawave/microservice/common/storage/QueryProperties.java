package datawave.microservice.common.storage;


import datawave.webservice.query.Query;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;


public class QueryProperties {
    public enum QUERY_STATE {DEFINED, CREATED, CLOSED, CANCELLED}

    private QUERY_STATE queryState = QUERY_STATE.DEFINED;
    private Query query;
    private String plan;

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

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(queryState).append(query).append(plan).build();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueryStats) {
            QueryProperties other = (QueryProperties)obj;
            return new EqualsBuilder().append(queryState, other.queryState).append(query, other.query).append(plan, other.plan).build();
        }
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("queryState", queryState).append("query", query).append("plan", plan).build();
    }
}
