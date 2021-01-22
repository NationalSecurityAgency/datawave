package datawave.webservice.query;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.MultivaluedMap;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(QueryImpl.class)
public abstract class Query implements Externalizable {
    
    private static final long serialVersionUID = -5980134700364340930L;
    
    public abstract void initialize(String userDN, List<String> dnList, String queryLogicName, QueryParameters qp,
                    MultivaluedMap<String,String> optionalQueryParameters);
    
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
    
    public abstract MultivaluedMap<String,String> toMap();
    
    public abstract void readMap(MultivaluedMap<String,String> map) throws ParseException;
    
    public abstract Map<String,String> getCardinalityFields();
    
    public abstract void populateMetric(BaseQueryMetric metric);
    
    public abstract void setOptionalQueryParameters(MultivaluedMap<String,String> optionalQueryParameters);
    
    public abstract MultivaluedMap<String,String> getOptionalQueryParameters();
    
    public abstract void removeParameter(String key);
    
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        MultivaluedMap<String,String> map = new MultivaluedMapImpl<>();
        int numKeys = in.readInt();
        for (int i = 0; i < numKeys; i++) {
            String key = in.readUTF();
            int numValues = in.readInt();
            for (int j = 0; j < numValues; j++) {
                String value = in.readUTF();
                map.add(key, value);
            }
        }
        try {
            readMap(map);
        } catch (ParseException pe) {
            throw new IOException("Could not parse value", pe);
        }
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        MultivaluedMap<String,String> map = toMap();
        Set<String> keys = map.keySet();
        out.writeInt(keys.size());
        for (String key : keys) {
            out.writeUTF(key);
            List<String> values = map.get(key);
            out.writeInt(values.size());
            for (String value : values) {
                out.writeUTF(value);
            }
        }
    }
}
