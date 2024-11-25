package datawave.microservice.query.cachedresults.status;

import static datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus.CACHED_RESULTS_STATE.LOADING;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import datawave.microservice.query.Query;
import datawave.security.authorization.ProxiedUserDetails;

public class CachedResultsQueryStatus implements Serializable {
    private static final long serialVersionUID = -3172313455309782821L;
    
    public enum CACHED_RESULTS_STATE {
        NONE, LOADING, LOADED, CREATING, CREATED, CANCELED, FAILED
    }
    
    private CACHED_RESULTS_STATE state;
    private String definedQueryId;
    private String cachedQueryId;
    private String alias;
    private String queryLogicName;
    private String origQuery;
    private String runningQueryId;
    private Query query;
    private String tableName;
    private String view;
    private Map<String,Integer> fieldIndexMap;
    private int rowsWritten;
    private String fields;
    private String conditions;
    private String grouping;
    private String order;
    private int pageSize = 10;
    private Set<String> fixedFields = new HashSet<>();
    private String sqlQuery;
    private ProxiedUserDetails currentUser;
    private long lastUpdatedMillis;
    
    public CachedResultsQueryStatus(String definedQueryId, String cachedQueryId, String alias, ProxiedUserDetails currentUser) {
        this.definedQueryId = definedQueryId;
        this.cachedQueryId = cachedQueryId;
        this.alias = alias;
        this.currentUser = currentUser;
        this.state = LOADING;
        this.lastUpdatedMillis = System.currentTimeMillis();
    }
    
    public CACHED_RESULTS_STATE getState() {
        return state;
    }
    
    public void setState(CACHED_RESULTS_STATE state) {
        this.state = state;
    }
    
    public String getDefinedQueryId() {
        return definedQueryId;
    }
    
    public void setDefinedQueryId(String definedQueryId) {
        this.definedQueryId = definedQueryId;
    }
    
    public String getCachedQueryId() {
        return cachedQueryId;
    }
    
    public void setCachedQueryId(String cachedQueryId) {
        this.cachedQueryId = cachedQueryId;
    }
    
    public String getAlias() {
        return alias;
    }
    
    public void setAlias(String alias) {
        this.alias = alias;
    }
    
    public String getQueryLogicName() {
        return queryLogicName;
    }
    
    public void setQueryLogicName(String queryLogicName) {
        this.queryLogicName = queryLogicName;
    }
    
    public String getOrigQuery() {
        return origQuery;
    }
    
    public void setOrigQuery(String origQuery) {
        this.origQuery = origQuery;
    }
    
    public String getRunningQueryId() {
        return runningQueryId;
    }
    
    public void setRunningQueryId(String runningQueryId) {
        this.runningQueryId = runningQueryId;
    }
    
    public Query getQuery() {
        return query;
    }
    
    public void setQuery(Query query) {
        this.query = query;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public String getView() {
        return view;
    }
    
    public void setView(String view) {
        this.view = view;
    }
    
    public Map<String,Integer> getFieldIndexMap() {
        return fieldIndexMap;
    }
    
    public void setFieldIndexMap(Map<String,Integer> fieldIndexMap) {
        this.fieldIndexMap = fieldIndexMap;
    }
    
    public int getRowsWritten() {
        return rowsWritten;
    }
    
    public void setRowsWritten(int rowsWritten) {
        this.rowsWritten = rowsWritten;
    }
    
    public String getFields() {
        return fields;
    }
    
    public void setFields(String fields) {
        this.fields = fields;
    }
    
    public String getConditions() {
        return conditions;
    }
    
    public void setConditions(String conditions) {
        this.conditions = conditions;
    }
    
    public String getGrouping() {
        return grouping;
    }
    
    public void setGrouping(String grouping) {
        this.grouping = grouping;
    }
    
    public String getOrder() {
        return order;
    }
    
    public void setOrder(String order) {
        this.order = order;
    }
    
    public int getPageSize() {
        return pageSize;
    }
    
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
    
    public Set<String> getFixedFields() {
        return fixedFields;
    }
    
    public void setFixedFields(Set<String> fixedFields) {
        this.fixedFields = fixedFields;
    }
    
    public String getSqlQuery() {
        return sqlQuery;
    }
    
    public void setSqlQuery(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }
    
    public ProxiedUserDetails getCurrentUser() {
        return currentUser;
    }
    
    public void setCurrentUser(ProxiedUserDetails currentUser) {
        this.currentUser = currentUser;
    }
    
    public long getLastUpdatedMillis() {
        return lastUpdatedMillis;
    }
    
    public void setLastUpdatedMillis(long lastUpdatedMillis) {
        this.lastUpdatedMillis = lastUpdatedMillis;
    }
}
