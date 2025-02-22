package datawave.microservice.query;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.util.MultiValueMap;

import datawave.validation.ParameterValidator;

/**
 * QueryParameters passed in from a client, they are validated and passed through to the iterator stack as QueryOptions.
 *
 */
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
    String QUERY_POOL = "pool";
    String QUERY_MAX_CONCURRENT_TASKS = "maxConcurrentTasks";
    String QUERY_PLAN_EXPAND_FIELDS = "expand.fields";
    String QUERY_PLAN_EXPAND_VALUES = "expand.values";
    String QUERY_SYSTEM_FROM = "systemFrom";
    
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
    
    String getSystemFrom();
    
    void setSystemFrom(String systemFrom);
    
    String getPool();
    
    void setPool(String pool);
    
    int getMaxConcurrentTasks();
    
    void setMaxConcurrentTasks(int maxConcurrentTasks);
    
    boolean isMaxConcurrentTasksOverridden();
    
    void setExpandFields(boolean expandFields);
    
    boolean isExpandFields();
    
    void setExpandValues(boolean expandVues);
    
    boolean isExpandValues();
    
    Map<String,List<String>> getRequestHeaders();
    
    void setRequestHeaders(Map<String,List<String>> requestHeaders);
    
    MultiValueMap<String,String> getUnknownParameters(Map<String,List<String>> allQueryParameters);
    
    void clear();
    
}
