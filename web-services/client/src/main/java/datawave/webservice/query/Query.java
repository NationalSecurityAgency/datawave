package datawave.webservice.query;

import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(QueryImpl.class)
public abstract class Query {
    
    public abstract void initialize(String userDN, List<String> dnList, String queryLogicName, QueryParameters qp,
                    Map<String,List<String>> optionalQueryParameters);
    
    public abstract String getQueryLogicName();
    
    public abstract UUID getId();
    
    public abstract void setId(UUID id);
    
    public abstract String getQueryName();
    
    public abstract void setQueryName(String queryName);
    
    public abstract String getUserDN();
    
    public abstract void setUserDN(String userDN);
    
    public abstract String getQuery();
    
    public abstract void setQuery(String query);
    
    public abstract String getQueryAuthorizations();
    
    public abstract void setQueryAuthorizations(String authorizations);
    
    public abstract Date getExpirationDate();
    
    public abstract void setExpirationDate(Date expirationDate);
    
    public abstract int getPagesize();
    
    public abstract void setPagesize(int pagesize);
    
    public abstract int getPageTimeout();
    
    public abstract void setPageTimeout(int pageTimeout);
    
    public abstract long getMaxResultsOverride();
    
    public abstract void setMaxResultsOverride(long maxResults);
    
    public abstract boolean isMaxResultsOverridden();
    
    public abstract Set<Parameter> getParameters();
    
    public abstract void setParameters(Set<Parameter> params);
    
    public abstract void setQueryLogicName(String name);
    
    public abstract Date getBeginDate();
    
    public abstract void setBeginDate(Date beginDate);
    
    public abstract Date getEndDate();
    
    public abstract void setEndDate(Date endDate);
    
    public abstract Query duplicate(String newQueryName);
    
    public abstract Parameter findParameter(String parameter);
    
    public abstract void setParameters(Map<String,String> parameters);
    
    public abstract void addParameter(String key, String val);
    
    public abstract void addParameters(Map<String,String> parameters);
    
    public abstract void setDnList(List<String> dnList);
    
    public abstract List<String> getDnList();
    
    public abstract QueryUncaughtExceptionHandler getUncaughtExceptionHandler();
    
    public abstract void setUncaughtExceptionHandler(QueryUncaughtExceptionHandler uncaughtExceptionHandler);
    
    public abstract void setOwner(String owner);
    
    public abstract String getOwner();
    
    public abstract void setColumnVisibility(String colviz);
    
    public abstract String getColumnVisibility();
    
    public abstract Map<String,List<String>> toMap();
    
    public abstract Map<String,String> getCardinalityFields();
    
    public abstract void setOptionalQueryParameters(Map<String,List<String>> optionalQueryParameters);
    
    public abstract Map<String,List<String>> getOptionalQueryParameters();
    
    public abstract void removeParameter(String key);
}
