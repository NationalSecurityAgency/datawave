package datawave.webservice.query.configuration;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import datawave.webservice.query.logic.BaseQueryLogic;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.collect.Iterators;

/**
 * <p>
 * A basic query configuration object that contains the information needed to run a query.
 * </p>
 * 
 * <p>
 * Provides some "expected" default values for parameters. This configuration object also encapsulates iterators and their options that would be set on a
 * {@link BatchScanner}.
 * </p>
 * 
 */
public abstract class GenericQueryConfiguration {
    private Connector connector = null;
    private Set<Authorizations> authorizations = Collections.singleton(Authorizations.EMPTY);
    // Leave in a top-level query for backwards-compatibility purposes
    private String queryString = null;
    
    private Date beginDate = null;
    private Date endDate = null;
    
    private Long maxRowsToScan = 25000l;
    
    private Set<String> undisplayedVisibilities = new HashSet<>();
    protected int baseIteratorPriority = 100;
    
    // Table name
    private String tableName = "shard";
    
    private Iterator<QueryData> queries = Iterators.emptyIterator();
    
    /**
     * Empty default constructor
     */
    public GenericQueryConfiguration() {
        
    }
    
    /**
     * Pulls the table name, max query results, and max rows to scan from the provided argument
     * 
     * @param configuredLogic
     *            A pre-configured BaseQueryLogic to initialize the Configuration with
     */
    public GenericQueryConfiguration(BaseQueryLogic<?> configuredLogic) {
        this.setTableName(configuredLogic.getTableName());
        this.setMaxRowsToScan(configuredLogic.getMaxRowsToScan());
        this.setUndisplayedVisibilities(configuredLogic.getUndisplayedVisibilities());
        this.setBaseIteratorPriority(configuredLogic.getBaseIteratorPriority());
    }
    
    /**
     * Return the configured {@code Iterator<QueryData>}
     * 
     * @return
     */
    public Iterator<QueryData> getQueries() {
        return Iterators.unmodifiableIterator(this.queries);
    }
    
    /**
     * Set the queries to be run.
     * 
     * @param queries
     */
    public void setQueries(Iterator<QueryData> queries) {
        this.queries = queries;
    }
    
    public Connector getConnector() {
        return connector;
    }
    
    public void setConnector(Connector connector) {
        this.connector = connector;
    }
    
    public void setQueryString(String query) {
        this.queryString = query;
    }
    
    public String getQueryString() {
        return queryString;
    }
    
    public Set<Authorizations> getAuthorizations() {
        return authorizations;
    }
    
    public void setAuthorizations(Set<Authorizations> auths) {
        this.authorizations = auths;
    }
    
    public int getBaseIteratorPriority() {
        return baseIteratorPriority;
    }
    
    public void setBaseIteratorPriority(final int baseIteratorPriority) {
        this.baseIteratorPriority = baseIteratorPriority;
    }
    
    public Date getBeginDate() {
        return beginDate;
    }
    
    public void setBeginDate(Date beginDate) {
        this.beginDate = beginDate;
    }
    
    public Date getEndDate() {
        return endDate;
    }
    
    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }
    
    public Long getMaxRowsToScan() {
        return maxRowsToScan;
    }
    
    public void setMaxRowsToScan(Long maxRowsToScan) {
        this.maxRowsToScan = maxRowsToScan <= 0l ? Long.MAX_VALUE : maxRowsToScan;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public Set<String> getUndisplayedVisibilities() {
        return undisplayedVisibilities;
    }
    
    public void setUndisplayedVisibilities(Set<String> undisplayedVisibilities) {
        this.undisplayedVisibilities = undisplayedVisibilities;
    }
    
    /**
     * Checks for non-null, sane values for the configured values
     * 
     * @return True if all of the encapsulated values have legitimate values, otherwise false
     */
    public boolean canRunQuery() {
        // Ensure we were given connector and authorizations
        if (null == this.getConnector() || null == this.getAuthorizations()) {
            return false;
        }
        
        // Ensure valid dates
        if (null == this.getBeginDate() || null == this.getEndDate() || endDate.before(beginDate)) {
            return false;
        }
        
        // A non-empty table was given
        if (null == getTableName() || this.getTableName().isEmpty()) {
            return false;
        }
        
        // At least one QueryData was provided
        if (null == this.queries) {
            return false;
        }
        
        return true;
    }
}
