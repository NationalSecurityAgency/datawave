package datawave.webservice.query;

import datawave.webservice.query.util.OptionallyEncodedStringAdapter;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import io.protostuff.UninitializedMessageException;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

@XmlRootElement(name = "QueryImpl")
@XmlAccessorType(XmlAccessType.NONE)
public class QueryImpl extends Query implements Serializable, Message<QueryImpl> {
    
    public static final String PARAMETER_SEPARATOR = ";";
    public static final String PARAMETER_NAME_VALUE_SEPARATOR = ":";
    
    @XmlAccessorType(XmlAccessType.FIELD)
    public static final class Parameter implements Serializable, Message<Parameter> {
        
        private static final long serialVersionUID = 2L;
        
        @XmlElement(name = "name")
        private String parameterName;
        @XmlElement(name = "value")
        private String parameterValue;
        
        public Parameter() {}
        
        public Parameter(String name, String value) {
            this.parameterName = name;
            this.parameterValue = value;
        }
        
        public String getParameterName() {
            return parameterName;
        }
        
        public void setParameterName(String parameterName) {
            this.parameterName = parameterName;
        }
        
        public String getParameterValue() {
            return parameterValue;
        }
        
        public void setParameterValue(String parameterValue) {
            this.parameterValue = parameterValue;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(256);
            sb.append("[name=").append(this.parameterName);
            sb.append(",value=").append(this.parameterValue).append("]");
            return sb.toString();
        }
        
        @Override
        public boolean equals(Object o) {
            if (null == o)
                return false;
            if (!(o instanceof Parameter))
                return false;
            if (this == o)
                return true;
            Parameter other = (Parameter) o;
            if (this.getParameterName().equals(other.getParameterName()) && this.getParameterValue().equals(other.getParameterValue()))
                return true;
            else
                return false;
        }
        
        @Override
        public int hashCode() {
            return getParameterName() == null ? 0 : getParameterName().hashCode();
        }
        
        @XmlTransient
        public static final Schema<Parameter> SCHEMA = new Schema<Parameter>() {
            public Parameter newMessage() {
                return new Parameter();
            }
            
            public Class<Parameter> typeClass() {
                return Parameter.class;
            }
            
            public String messageName() {
                return Parameter.class.getSimpleName();
            }
            
            public String messageFullName() {
                return Parameter.class.getName();
            }
            
            public boolean isInitialized(Parameter message) {
                return message.parameterName != null && message.parameterValue != null;
            }
            
            public void writeTo(Output output, Parameter message) throws IOException {
                if (message.parameterName == null)
                    throw new UninitializedMessageException(message);
                output.writeString(1, message.parameterName, false);
                
                if (message.parameterValue == null)
                    throw new UninitializedMessageException(message);
                output.writeString(2, message.parameterValue, false);
            }
            
            public void mergeFrom(Input input, Parameter message) throws IOException {
                int number;
                while ((number = input.readFieldNumber(this)) != 0) {
                    switch (number) {
                        case 1:
                            message.parameterName = input.readString();
                            break;
                        case 2:
                            message.parameterValue = input.readString();
                            break;
                        default:
                            input.handleUnknownField(number, this);
                            break;
                    }
                }
            }
            
            public String getFieldName(int number) {
                switch (number) {
                    case 1:
                        return "parameterName";
                    case 2:
                        return "parameterValue";
                    default:
                        return null;
                }
            }
            
            public int getFieldNumber(String name) {
                final Integer number = fieldMap.get(name);
                return number == null ? 0 : number.intValue();
            }
            
            final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<String,Integer>();
            {
                fieldMap.put("parameterName", 1);
                fieldMap.put("parameterValue", 2);
            }
        };
        
        public static Schema<Parameter> getSchema() {
            return SCHEMA;
        }
        
        @Override
        public Schema<Parameter> cachedSchema() {
            return SCHEMA;
        }
        
    }
    
    private static final long serialVersionUID = 2L;
    
    @XmlElement
    protected String queryLogicName;
    @XmlElement
    protected String id;
    @XmlElement
    protected String queryName;
    @XmlElement
    protected String userDN;
    @XmlElement
    @XmlJavaTypeAdapter(OptionallyEncodedStringAdapter.class)
    protected String query;
    @XmlElement
    protected Date beginDate;
    @XmlElement
    protected Date endDate;
    @XmlElement
    protected String queryAuthorizations;
    @XmlElement
    protected Date expirationDate;
    @XmlElement
    protected int pagesize;
    @XmlElement
    protected int pageTimeout;
    @XmlElement
    protected boolean isMaxResultsOverridden;
    @XmlElement
    protected long maxResultsOverride;
    @XmlElement
    protected HashSet<Parameter> parameters = new HashSet<Parameter>();
    @XmlElement
    protected List<String> dnList;
    @XmlElement
    protected String owner;
    @XmlElement
    protected String columnVisibility;
    @XmlTransient
    protected Map<String,List<String>> optionalQueryParameters;
    
    protected transient QueryUncaughtExceptionHandler uncaughtExceptionHandler;
    
    protected transient HashMap<String,Parameter> paramLookup = new HashMap<String,Parameter>();
    
    public String getQueryLogicName() {
        return queryLogicName;
    }
    
    public UUID getId() {
        if (null == id)
            return null;
        return UUID.fromString(id);
    }
    
    public String getQueryName() {
        return queryName;
    }
    
    public String getUserDN() {
        return userDN;
    }
    
    public String getQuery() {
        return query;
    }
    
    public String getQueryAuthorizations() {
        return queryAuthorizations;
    }
    
    public Date getExpirationDate() {
        return expirationDate;
    }
    
    public int getPagesize() {
        return pagesize;
    }
    
    public int getPageTimeout() {
        return pageTimeout;
    }
    
    public long getMaxResultsOverride() {
        return maxResultsOverride;
    }
    
    public boolean isMaxResultsOverridden() {
        return isMaxResultsOverridden;
    }
    
    public Set<Parameter> getParameters() {
        return parameters == null ? null : Collections.unmodifiableSet(parameters);
    }
    
    public void setQueryLogicName(String name) {
        this.queryLogicName = name;
    }
    
    public void setId(UUID id) {
        this.id = id.toString();
    }
    
    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }
    
    public void setUserDN(String userDN) {
        this.userDN = userDN;
    }
    
    public void setQuery(String query) {
        this.query = query;
    }
    
    public void setQueryAuthorizations(String queryAuthorizations) {
        this.queryAuthorizations = queryAuthorizations;
    }
    
    public void setExpirationDate(Date expirationDate) {
        this.expirationDate = expirationDate;
    }
    
    public void setMaxResultsOverride(long maxResults) {
        this.maxResultsOverride = maxResults;
        this.isMaxResultsOverridden = true;
    }
    
    public void setPagesize(int pagesize) {
        this.pagesize = pagesize;
    }
    
    public void setPageTimeout(int pageTimeout) {
        this.pageTimeout = pageTimeout;
    }
    
    public void setParameters(Set<Parameter> parameters) {
        this.parameters.clear();
        this.parameters.addAll(parameters);
        this.paramLookup.clear();
        for (Parameter p : this.parameters) {
            this.paramLookup.put(p.getParameterName(), p);
        }
    }
    
    public void addParameter(String key, String val) {
        Parameter p = new Parameter(key, val);
        this.parameters.add(p);
        this.paramLookup.put(p.getParameterName(), p);
    }
    
    public void addParameters(Map<String,String> parameters) {
        for (Entry<String,String> p : parameters.entrySet()) {
            addParameter(p.getKey(), p.getValue());
        }
    }
    
    public void setParameters(Map<String,String> parameters) {
        HashSet<Parameter> paramObjs = new HashSet<Parameter>(parameters.size());
        for (Entry<String,String> param : parameters.entrySet()) {
            Parameter p = new Parameter(param.getKey(), param.getValue());
            paramObjs.add(p);
        }
        this.setParameters(paramObjs);
    }
    
    public List<String> getDnList() {
        return dnList;
    }
    
    public void setDnList(List<String> dnList) {
        this.dnList = dnList;
    }
    
    public String getColumnVisibility() {
        return columnVisibility;
    }
    
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
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
    
    public Map<String,List<String>> getOptionalQueryParameters() {
        return optionalQueryParameters;
    }
    
    public void setOptionalQueryParameters(Map<String,List<String>> optionalQueryParameters) {
        this.optionalQueryParameters = optionalQueryParameters;
    }
    
    public QueryImpl duplicate(String newQueryName) {
        QueryImpl query = new QueryImpl();
        query.setQueryLogicName(this.getQueryLogicName());
        query.setQueryName(newQueryName);
        query.setExpirationDate(this.getExpirationDate());
        query.setId(UUID.randomUUID());
        query.setPagesize(this.getPagesize());
        query.setPageTimeout(this.getPageTimeout());
        if (query.isMaxResultsOverridden()) {
            query.setMaxResultsOverride(this.getMaxResultsOverride());
        }
        query.setQuery(this.getQuery());
        query.setQueryAuthorizations(this.getQueryAuthorizations());
        query.setUserDN(this.getUserDN());
        query.setOwner(this.getOwner());
        query.setColumnVisibility(this.getColumnVisibility());
        query.setBeginDate(this.getBeginDate());
        query.setEndDate(this.getEndDate());
        if (null != this.parameters && !this.parameters.isEmpty())
            query.setParameters(new HashSet<Parameter>(this.parameters));
        query.setDnList(this.dnList);
        return query;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(this.getQueryLogicName()).append(this.getQueryName()).append(this.getExpirationDate())
                        .append(UUID.randomUUID()).append(this.getPagesize()).append(this.getPageTimeout())
                        .append(this.isMaxResultsOverridden() ? this.getMaxResultsOverride() : 0).append(this.getQuery()).append(this.getQueryAuthorizations())
                        .append(this.getUserDN()).append(this.getOwner()).append(this.getParameters()).append(this.getDnList())
                        .append(this.getColumnVisibility()).append(this.getBeginDate()).append(this.getEndDate()).toHashCode();
    }
    
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("queryLogicName", this.getQueryLogicName());
        tsb.append("queryName", this.getQueryName());
        tsb.append("expirationDate", this.getExpirationDate());
        tsb.append("uuid", this.getId());
        tsb.append("pagesize", this.getPagesize());
        tsb.append("pageTimeout", this.getPageTimeout());
        tsb.append("maxResultsOverride", (this.isMaxResultsOverridden() ? this.getMaxResultsOverride() : "NA"));
        tsb.append("query", this.getQuery());
        tsb.append("queryAuthorizations", this.getQueryAuthorizations());
        tsb.append("userDN", this.getUserDN());
        tsb.append("owner", this.getOwner());
        tsb.append("parameters", this.getParameters());
        tsb.append("dnList", this.getDnList());
        tsb.append("columnVisibility", this.getColumnVisibility());
        tsb.append("beginDate", this.getBeginDate());
        tsb.append("endDate", this.getEndDate());
        return tsb.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (null == o)
            return false;
        if (!(o instanceof QueryImpl))
            return false;
        if (this == o)
            return true;
        QueryImpl other = (QueryImpl) o;
        EqualsBuilder eb = new EqualsBuilder();
        eb.append(this.getQueryLogicName(), other.getQueryLogicName());
        eb.append(this.getId(), other.getId());
        eb.append(this.getQueryName(), other.getQueryName());
        eb.append(this.getUserDN(), other.getUserDN());
        eb.append(this.getOwner(), other.getOwner());
        eb.append(this.getQuery(), other.getQuery());
        eb.append(this.getQueryAuthorizations(), other.getQueryAuthorizations());
        eb.append(this.getExpirationDate(), other.getExpirationDate());
        eb.append(this.getPagesize(), other.getPagesize());
        eb.append(this.getPageTimeout(), other.getPageTimeout());
        eb.append(this.isMaxResultsOverridden(), other.isMaxResultsOverridden());
        if (this.isMaxResultsOverridden()) {
            eb.append(this.getMaxResultsOverride(), other.getMaxResultsOverride());
        }
        eb.append(this.getColumnVisibility(), other.getColumnVisibility());
        eb.append(this.getBeginDate(), other.getBeginDate());
        eb.append(this.getEndDate(), other.getEndDate());
        eb.append(this.getDnList(), other.getDnList());
        eb.append(this.getParameters(), other.getParameters());
        return eb.isEquals();
    }
    
    public Parameter findParameter(String parameter) {
        if (!paramLookup.containsKey(parameter)) {
            return new Parameter(parameter, "");
        } else {
            return paramLookup.get(parameter);
        }
    }
    
    @XmlTransient
    private static final Schema<QueryImpl> SCHEMA = new Schema<QueryImpl>() {
        public QueryImpl newMessage() {
            return new QueryImpl();
        }
        
        public Class<QueryImpl> typeClass() {
            return QueryImpl.class;
        }
        
        public String messageName() {
            return QueryImpl.class.getSimpleName();
        }
        
        public String messageFullName() {
            return QueryImpl.class.getName();
        }
        
        public boolean isInitialized(QueryImpl message) {
            return message.queryLogicName != null && message.id != null && message.userDN != null && message.query != null
                            && message.queryAuthorizations != null && message.expirationDate != null && message.pagesize > 0 && message.pageTimeout != 0;
        }
        
        public void writeTo(Output output, QueryImpl message) throws IOException {
            if (message.queryLogicName == null)
                throw new UninitializedMessageException(message, SCHEMA);
            output.writeString(1, message.queryLogicName, false);
            
            if (message.id == null)
                throw new UninitializedMessageException(message, SCHEMA);
            output.writeString(2, message.id, false);
            
            if (message.queryName != null)
                output.writeString(3, message.queryName, false);
            
            if (message.userDN == null)
                throw new UninitializedMessageException(message, SCHEMA);
            output.writeString(4, message.userDN, false);
            
            if (message.query == null)
                throw new UninitializedMessageException(message, SCHEMA);
            output.writeString(5, message.query, false);
            
            if (message.beginDate != null)
                output.writeInt64(6, message.beginDate.getTime(), false);
            
            if (message.endDate != null)
                output.writeInt64(7, message.endDate.getTime(), false);
            
            if (message.queryAuthorizations == null)
                throw new UninitializedMessageException(message, SCHEMA);
            output.writeString(8, message.queryAuthorizations, false);
            
            if (message.expirationDate == null)
                throw new UninitializedMessageException(message, SCHEMA);
            output.writeInt64(9, message.expirationDate.getTime(), false);
            
            if (message.pagesize <= 0)
                throw new UninitializedMessageException(message, SCHEMA);
            output.writeUInt32(10, message.pagesize, false);
            
            if (message.parameters != null) {
                for (Parameter p : message.parameters) {
                    output.writeObject(11, p, Parameter.SCHEMA, true);
                }
            }
            
            if (message.owner == null)
                throw new UninitializedMessageException(message, SCHEMA);
            output.writeString(12, message.owner, false);
            
            if (null != message.dnList) {
                for (String dn : message.dnList)
                    output.writeString(13, dn, true);
            }
            
            if (message.columnVisibility != null) {
                output.writeString(14, message.columnVisibility, false);
            }
            
            if (message.pageTimeout == 0)
                throw new UninitializedMessageException(message, SCHEMA);
            output.writeUInt32(15, message.pageTimeout, false);
        }
        
        public void mergeFrom(Input input, QueryImpl message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.queryLogicName = input.readString();
                        break;
                    case 2:
                        message.id = input.readString();
                        break;
                    case 3:
                        message.queryName = input.readString();
                        break;
                    case 4:
                        message.userDN = input.readString();
                        break;
                    case 5:
                        message.query = input.readString();
                        break;
                    
                    case 6:
                        message.beginDate = new Date(input.readInt64());
                        break;
                    case 7:
                        message.endDate = new Date(input.readInt64());
                        break;
                    case 8:
                        message.queryAuthorizations = input.readString();
                        break;
                    case 9:
                        message.expirationDate = new Date(input.readInt64());
                        break;
                    case 10:
                        message.pagesize = input.readUInt32();
                        break;
                    case 11:
                        if (message.parameters == null)
                            message.parameters = new HashSet<Parameter>();
                        Parameter p = input.mergeObject(null, Parameter.SCHEMA);
                        message.addParameter(p.getParameterName(), p.getParameterValue());
                        break;
                    case 12:
                        message.owner = input.readString();
                        break;
                    case 13:
                        if (null == message.dnList)
                            message.dnList = new ArrayList<String>();
                        message.dnList.add(input.readString());
                        break;
                    case 14:
                        message.columnVisibility = input.readString();
                        break;
                    case 15:
                        message.pageTimeout = input.readUInt32();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }
        
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "queryLogicName";
                case 2:
                    return "id";
                case 3:
                    return "queryName";
                case 4:
                    return "userDN";
                case 5:
                    return "query";
                case 6:
                    return "beginDate";
                case 7:
                    return "endDate";
                case 8:
                    return "queryAuthorizations";
                case 9:
                    return "expirationDate";
                case 10:
                    return "pagesize";
                case 11:
                    return "parameters";
                case 12:
                    return "owner";
                case 13:
                    return "dnList";
                case 14:
                    return "columnVisibility";
                case 15:
                    return "pageTimeout";
                default:
                    return null;
            }
        }
        
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }
        
        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<String,Integer>();
        {
            fieldMap.put("queryLogicName", 1);
            fieldMap.put("id", 2);
            fieldMap.put("queryName", 3);
            fieldMap.put("userDN", 4);
            fieldMap.put("query", 5);
            fieldMap.put("beginDate", 6);
            fieldMap.put("endDate", 7);
            fieldMap.put("queryAuthorizations", 8);
            fieldMap.put("expirationDate", 9);
            fieldMap.put("pagesize", 10);
            fieldMap.put("parameters", 11);
            fieldMap.put("owner", 12);
            fieldMap.put("dnList", 13);
            fieldMap.put("columnVisibility", 14);
            fieldMap.put("pageTimeout", 15);
        }
    };
    
    public QueryUncaughtExceptionHandler getUncaughtExceptionHandler() {
        return this.uncaughtExceptionHandler;
    }
    
    public void setUncaughtExceptionHandler(QueryUncaughtExceptionHandler uncaughtExceptionHandler) {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    }
    
    public void initialize(String userDN, List<String> dnList, String queryLogicName, QueryParameters qp, Map<String,List<String>> optionalQueryParameters) {
        this.dnList = dnList;
        this.expirationDate = qp.getExpirationDate();
        this.id = UUID.randomUUID().toString();
        this.pagesize = qp.getPagesize();
        this.pageTimeout = qp.getPageTimeout();
        this.query = qp.getQuery();
        this.queryAuthorizations = qp.getAuths();
        this.queryLogicName = queryLogicName;
        this.queryName = qp.getQueryName();
        this.userDN = userDN;
        this.owner = getOwner(this.userDN);
        this.beginDate = qp.getBeginDate();
        this.endDate = qp.getEndDate();
        if (optionalQueryParameters != null) {
            for (Entry<String,List<String>> entry : optionalQueryParameters.entrySet()) {
                if (entry.getValue().get(0) != null) {
                    this.addParameter(entry.getKey(), entry.getValue().get(0));
                }
            }
        }
    }
    
    private static String getCommonName(String dn) {
        String[] comps = getComponents(dn, "CN");
        return comps.length >= 1 ? comps[0] : null;
    }
    
    private static String[] getComponents(String dn, String componentName) {
        componentName = componentName.toUpperCase();
        ArrayList<String> components = new ArrayList<String>();
        try {
            LdapName name = new LdapName(dn);
            for (Rdn rdn : name.getRdns()) {
                if (componentName.equals(rdn.getType().toUpperCase())) {
                    components.add(String.valueOf(rdn.getValue()));
                }
            }
        } catch (InvalidNameException e) {
            // ignore -- invalid name, so can't find components
        }
        return components.toArray(new String[0]);
    }
    
    public static String getOwner(String dn) {
        String sid = null;
        if (dn != null) {
            String cn = getCommonName(dn);
            if (cn == null)
                cn = dn;
            sid = cn;
            int idx = cn.lastIndexOf(' ');
            if (idx >= 0)
                sid = cn.substring(idx + 1);
        }
        return sid;
    }
    
    public void setOwner(String owner) {
        this.owner = owner;
    }
    
    public String getOwner() {
        return this.owner;
    }
    
    public Map<String,List<String>> toMap() {
        // TODO: missing variables uuid and owner -- not going into map
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        if (this.queryAuthorizations != null) {
            p.set(QueryParameters.QUERY_AUTHORIZATIONS, this.queryAuthorizations);
        }
        if (this.expirationDate != null) {
            try {
                p.set(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(this.expirationDate));
            } catch (ParseException e) {
                throw new RuntimeException("Error formatting date", e);
            }
        }
        if (this.queryName != null) {
            p.set(QueryParameters.QUERY_NAME, this.queryName);
        }
        if (this.queryLogicName != null) {
            p.set(QueryParameters.QUERY_LOGIC_NAME, this.queryLogicName);
        }
        // no null check on primitives
        p.set(QueryParameters.QUERY_PAGESIZE, Integer.toString(this.pagesize));
        if (this.query != null) {
            p.set(QueryParameters.QUERY_STRING, this.query);
        }
        if (this.userDN != null) {
            p.set("userDN", this.userDN);
        }
        if (this.dnList != null) {
            p.set("dnList", this.dnList.toString());
        }
        if (this.columnVisibility != null) {
            p.set("columnVisibility", this.columnVisibility);
        }
        if (this.beginDate != null) {
            try {
                p.set(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(this.beginDate));
            } catch (ParseException e) {
                throw new RuntimeException("Error formatting date", e);
            }
        }
        if (this.endDate != null) {
            try {
                p.set(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(this.endDate));
            } catch (ParseException e) {
                throw new RuntimeException("Error formatting date", e);
            }
        }
        if (this.parameters != null) {
            for (Parameter parameter : parameters) {
                p.set(parameter.getParameterName(), parameter.getParameterValue());
            }
        }
        return p;
    }
    
    @Override
    public Map<String,String> getCardinalityFields() {
        Map<String,String> cardinalityFields = new HashMap<String,String>();
        cardinalityFields.put("QUERY_USER", getOwner());
        cardinalityFields.put("QUERY_LOGIC_NAME", getQueryLogicName());
        return cardinalityFields;
    }
    
    @Override
    public Schema<QueryImpl> cachedSchema() {
        return SCHEMA;
    }
    
    @Override
    public void removeParameter(String key) {
        this.parameters.remove(paramLookup.get(key));
        this.paramLookup.remove(key);
    }
}
