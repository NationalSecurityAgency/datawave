package datawave.webservice.query.hud;

import java.util.ArrayList;
import java.util.List;

import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;

/**
 * 
 */
public class HudQuerySummary {
    private String queryLogicName;
    private String id;
    private String queryName;
    private String userDN;
    private String query;
    private String columnVisibility;
    private long beginDate;
    private long endDate;
    private String queryAuthorizations;
    private long expirationDate;
    private String sid;
    private List<PageMetric> pageMetrics;
    
    private long createDate;
    private long numPages;
    private long numResults;
    private long lastUpdated;
    private String lifeCycle;
    
    /**
     * @return the queryLogicName
     */
    public String getQueryLogicName() {
        return queryLogicName;
    }
    
    /**
     * @param queryLogicName
     *            the queryLogicName to set
     */
    public void setQueryLogicName(String queryLogicName) {
        this.queryLogicName = queryLogicName;
    }
    
    /**
     * @return the id
     */
    public String getId() {
        return id;
    }
    
    /**
     * @param id
     *            the id to set
     */
    public void setId(String id) {
        this.id = id;
    }
    
    /**
     * @return the queryName
     */
    public String getQueryName() {
        return queryName;
    }
    
    /**
     * @param queryName
     *            the queryName to set
     */
    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }
    
    /**
     * @return the userDN
     */
    public String getUserDN() {
        return userDN;
    }
    
    /**
     * @param userDN
     *            the userDN to set
     */
    public void setUserDN(String userDN) {
        this.userDN = userDN;
    }
    
    /**
     * @return the query
     */
    public String getQuery() {
        return query;
    }
    
    /**
     * @param query
     *            the query to set
     */
    public void setQuery(String query) {
        this.query = query;
    }
    
    /**
     * @return the columnVisibility
     */
    public String getColumnVisibility() {
        return columnVisibility;
    }
    
    /**
     * @param columnVisibility
     *            the columnVisibility to set
     */
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
    
    /**
     * @return the beginDate
     */
    public long getBeginDate() {
        return beginDate;
    }
    
    /**
     * @param beginDate
     *            the beginDate to set
     */
    public void setBeginDate(long beginDate) {
        this.beginDate = beginDate;
    }
    
    /**
     * @return the endDate
     */
    public long getEndDate() {
        return endDate;
    }
    
    /**
     * @param endDate
     *            the endDate to set
     */
    public void setEndDate(long endDate) {
        this.endDate = endDate;
    }
    
    /**
     * @return the queryAuthorizations
     */
    public String getQueryAuthorizations() {
        return queryAuthorizations;
    }
    
    /**
     * @param queryAuthorizations
     *            the queryAuthorizations to set
     */
    public void setQueryAuthorizations(String queryAuthorizations) {
        this.queryAuthorizations = queryAuthorizations;
    }
    
    /**
     * @return the expirationDate
     */
    public long getExpirationDate() {
        return expirationDate;
    }
    
    /**
     * @param expirationDate
     *            the expirationDate to set
     */
    public void setExpirationDate(long expirationDate) {
        this.expirationDate = expirationDate;
    }
    
    /**
     * @return the sid
     */
    public String getSid() {
        return sid;
    }
    
    /**
     * @param sid
     *            the sid to set
     */
    public void setSid(String sid) {
        this.sid = sid;
    }
    
    /**
     * @return the createDate
     */
    public long getCreateDate() {
        return createDate;
    }
    
    /**
     * @param createDate
     *            the createDate to set
     */
    public void setCreateDate(long createDate) {
        this.createDate = createDate;
    }
    
    /**
     * @return the numPages
     */
    public long getNumPages() {
        return numPages;
    }
    
    /**
     * @param numPages
     *            the numPages to set
     */
    public void setNumPages(long numPages) {
        this.numPages = numPages;
    }
    
    /**
     * @return the numResults
     */
    public long getNumResults() {
        return numResults;
    }
    
    /**
     * @param numResults
     *            the numResults to set
     */
    public void setNumResults(long numResults) {
        this.numResults = numResults;
    }
    
    /**
     * @return the lastUpdated
     */
    public long getLastUpdated() {
        return lastUpdated;
    }
    
    /**
     * @param lastUpdated
     *            the lastUpdated to set
     */
    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    /**
     * @return the lifeCycle
     */
    public String getLifeCycle() {
        return lifeCycle;
    }
    
    /**
     * @param lifeCycle
     *            the lifeCycle to set
     */
    public void setLifeCycle(String lifeCycle) {
        this.lifeCycle = lifeCycle;
    }
    
    /**
     * @return the pageMetrics
     */
    public List<PageMetric> getPageMetrics() {
        return pageMetrics;
    }
    
    /**
     * @param otherPageMetrics
     *            the pageMetrics to set
     */
    public void setPageMetrics(List<PageMetric> otherPageMetrics) {
        this.pageMetrics = new ArrayList<>(otherPageMetrics.size());
        this.pageMetrics.addAll(otherPageMetrics);
        
    }
    
}
