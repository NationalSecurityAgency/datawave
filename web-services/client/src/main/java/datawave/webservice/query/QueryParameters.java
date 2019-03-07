package datawave.webservice.query;

import java.util.Date;

import javax.ws.rs.core.MultivaluedMap;

import datawave.validation.ParameterValidator;

public interface QueryParameters extends ParameterValidator {
    
    String QUERY_STRING = "query";
    String QUERY_NAME = "queryName";
    String QUERY_PERSISTENCE = "persistence";
    String QUERY_PAGESIZE = "pagesize";
    String QUERY_PAGETIMEOUT = "pageTimeout";
    String QUERY_MAX_RESULTS_OVERRIDE = "max.results.override";
    String QUERY_AUTHORIZATIONS = "auths";
    String QUERY_EXPIRATION = "expiration";
    String QUERY_TRACE = "trace";
    String QUERY_BEGIN = "begin";
    String QUERY_END = "end";
    String QUERY_PARAMS = "params";
    String QUERY_VISIBILITY = "columnVisibility";
    String QUERY_LOGIC_NAME = "logicName";
    
    String getQuery();
    
    void setQuery(String query);
    
    String getQueryName();
    
    void setQueryName(String queryName);
    
    QueryPersistence getPersistenceMode();
    
    void setPersistenceMode(QueryPersistence persistenceMode);
    
    int getPagesize();
    
    void setPagesize(int pagesize);
    
    int getPageTimeout();
    
    void setPageTimeout(int pageTimeout);
    
    long getMaxResultsOverride();
    
    void setMaxResultsOverride(long maxResults);
    
    boolean isMaxResultsOverridden();
    
    String getAuths();
    
    void setAuths(String auths);
    
    Date getExpirationDate();
    
    void setExpirationDate(Date expirationDate);
    
    boolean isTrace();
    
    void setTrace(boolean trace);
    
    Date getBeginDate();
    
    Date getEndDate();
    
    void setBeginDate(Date beginDate);
    
    void setEndDate(Date endDate);
    
    String getVisibility();
    
    void setVisibility(String visibility);
    
    String getLogicName();
    
    void setLogicName(String logicName);
    
    MultivaluedMap<String,String> getRequestHeaders();
    
    void setRequestHeaders(MultivaluedMap<String,String> requestHeaders);
    
    MultivaluedMap<String,String> getUnknownParameters(MultivaluedMap<String,String> allQueryParameters);
    
    void clear();
    
}
