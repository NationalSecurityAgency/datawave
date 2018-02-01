package datawave.webservice.query;

import java.util.Date;

import javax.ws.rs.core.MultivaluedMap;

import datawave.validation.ParameterValidator;

public interface QueryParameters extends ParameterValidator {
    
    public static final String QUERY_STRING = "query";
    public static final String QUERY_NAME = "queryName";
    public static final String QUERY_PERSISTENCE = "persistence";
    public static final String QUERY_PAGESIZE = "pagesize";
    public static final String QUERY_PAGETIMEOUT = "pageTimeout";
    public static final String QUERY_AUTHORIZATIONS = "auths";
    public static final String QUERY_EXPIRATION = "expiration";
    public static final String QUERY_TRACE = "trace";
    public static final String QUERY_BEGIN = "begin";
    public static final String QUERY_END = "end";
    public static final String QUERY_PARAMS = "params";
    public static final String QUERY_VISIBILITY = "columnVisibility";
    public static final String QUERY_LOGIC_NAME = "logicName";
    
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
