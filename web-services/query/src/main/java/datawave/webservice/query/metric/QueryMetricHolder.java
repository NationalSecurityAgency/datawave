package datawave.webservice.query.metric;

import java.io.Serializable;

import datawave.microservice.querymetric.BaseQueryMetric;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import datawave.security.authorization.DatawavePrincipal;

/**
 * 
 */
public class QueryMetricHolder implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private DatawavePrincipal principal = null;
    private BaseQueryMetric queryMetric = null;
    
    public QueryMetricHolder(DatawavePrincipal principal, BaseQueryMetric queryMetric) {
        this.principal = principal;
        this.queryMetric = queryMetric;
    }
    
    public BaseQueryMetric getQueryMetric() {
        return queryMetric;
    }
    
    public void setQueryMetric(BaseQueryMetric queryMetric) {
        this.queryMetric = queryMetric;
    }
    
    public DatawavePrincipal getPrincipal() {
        return principal;
    }
    
    public void setPrincipal(DatawavePrincipal principal) {
        this.principal = principal;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(this.queryMetric).append(this.principal).toHashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (null == o)
            return false;
        if (!(o instanceof QueryMetricHolder))
            return false;
        QueryMetricHolder other = (QueryMetricHolder) o;
        return new EqualsBuilder().append(this.queryMetric, other.queryMetric).append(this.principal, other.principal).isEquals();
    }
    
    @Override
    public String toString() {
        if (this.queryMetric == null || this.principal == null) {
            return null;
        } else {
            return this.principal + ":" + this.queryMetric;
        }
    }
    
}
