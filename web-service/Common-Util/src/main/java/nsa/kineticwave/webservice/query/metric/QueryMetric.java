package nsa.kineticwave.webservice.query.metric;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class QueryMetric implements Serializable, Message<QueryMetric> {
    
    private static final long serialVersionUID = 1L;
    
    @XmlAccessorType(XmlAccessType.NONE)
    public static class PageMetric implements Serializable, Message<PageMetric> {
        private static final long serialVersionUID = 1L;
        @XmlElement
        private long pagesize = 0;
        @XmlElement
        private long returnTime = 0;
        
        public PageMetric() {
            super();
        }
        
        public PageMetric(long pagesize, long returnTime) {
            super();
            this.pagesize = pagesize;
            this.returnTime = returnTime;
        }
        
        public long getPagesize() {
            return pagesize;
        }
        
        public long getReturnTime() {
            return returnTime;
        }
        
        public void setPagesize(long pagesize) {
            this.pagesize = pagesize;
        }
        
        public void setReturnTime(long returnTime) {
            this.returnTime = returnTime;
        }
        
        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(this.pagesize).append(this.returnTime).toHashCode();
        }
        
        @Override
        public boolean equals(Object o) {
            if (null == o)
                return false;
            if (this == o)
                return true;
            if (o instanceof PageMetric) {
                PageMetric other = (PageMetric) o;
                if (this.getPagesize() == other.getPagesize() && this.getReturnTime() == other.getReturnTime())
                    return true;
                else
                    return false;
            } else
                return false;
        }
        
        @Override
        public String toString() {
            return new StringBuilder().append("Pagesize: ").append(this.pagesize).append(" ReturnTime(ms): ").append(this.returnTime).toString();
        }
        
        public static Schema<PageMetric> getSchema() {
            return SCHEMA;
        }
        
        @Override
        public Schema<PageMetric> cachedSchema() {
            return SCHEMA;
        }
        
        private static final Schema<PageMetric> SCHEMA = new Schema<PageMetric>() {
            public PageMetric newMessage() {
                return new PageMetric();
            }
            
            public Class<PageMetric> typeClass() {
                return PageMetric.class;
            }
            
            public String messageName() {
                return PageMetric.class.getSimpleName();
            }
            
            public String messageFullName() {
                return PageMetric.class.getName();
            }
            
            public boolean isInitialized(PageMetric message) {
                return true;
            }
            
            public void writeTo(Output output, PageMetric message) throws IOException {
                output.writeUInt64(1, message.pagesize, false);
                output.writeUInt64(2, message.returnTime, false);
            }
            
            public void mergeFrom(Input input, PageMetric message) throws IOException {
                int number;
                while ((number = input.readFieldNumber(this)) != 0) {
                    switch (number) {
                        case 1:
                            message.pagesize = input.readUInt64();
                            break;
                        case 2:
                            message.returnTime = input.readUInt64();
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
                        return "pagesize";
                    case 2:
                        return "returnTime";
                    default:
                        return null;
                }
            }
            
            public int getFieldNumber(String name) {
                final Integer number = fieldMap.get(name);
                return number == null ? 0 : number.intValue();
            }
            
            final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<>();
            {
                fieldMap.put("pagesize", 1);
                fieldMap.put("returnTime", 2);
            }
        };
    }
    
    @XmlElement
    private String queryType = null;
    @XmlElement
    private String user = null;
    @XmlElement
    private Date createDate = null;
    @XmlElement
    private String queryId = null;
    @XmlElement
    private long setupTime = 0;
    @XmlElement
    private String query = null;
    @XmlElement
    private String host = null;
    @XmlElement
    private ArrayList<PageMetric> pageTimes = new ArrayList<>();
    
    public QueryMetric() {
        super();
        this.createDate = new Date();
        this.host = System.getProperty("jboss.host.name");
    }
    
    public String getQueryType() {
        return queryType;
    }
    
    public String getUser() {
        return user;
    }
    
    public Date getCreateDate() {
        return (Date) createDate.clone();
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    public long getSetupTime() {
        return setupTime;
    }
    
    public String getQuery() {
        return query;
    }
    
    public String getHost() {
        return host;
    }
    
    public List<PageMetric> getPageTimes() {
        return pageTimes;
    }
    
    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }
    
    public void setQueryType(Class<?> queryType) {
        this.queryType = queryType.getSimpleName();
    }
    
    public void setUser(String user) {
        this.user = user;
    }
    
    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
    
    public void setSetupTime(long setupTime) {
        this.setupTime = setupTime;
    }
    
    public void setQuery(String query) {
        this.query = query;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public void addPageTime(long pagesize, long time) {
        this.pageTimes.add(new PageMetric(pagesize, time));
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(this.getCreateDate()).append(this.getQueryId()).append(this.getQueryType()).append(this.getSetupTime())
                        .append(this.getUser()).append(this.getQuery()).append(this.getHost()).append(this.getPageTimes()).toHashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (null == o)
            return false;
        if (this == o)
            return true;
        if (o instanceof QueryMetric) {
            QueryMetric other = (QueryMetric) o;
            return new EqualsBuilder().append(this.getQueryId(), other.getQueryId()).append(this.getQueryType(), other.getQueryType())
                            .append(this.getCreateDate(), other.getCreateDate()).append(this.getSetupTime(), other.getSetupTime())
                            .append(this.getUser(), other.getUser()).append(this.getQuery(), other.getQuery()).append(this.getHost(), other.getHost())
                            .append(this.getPageTimes(), other.getPageTimes()).isEquals();
        } else
            return false;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Type: ").append(this.queryType);
        buf.append(" User: ").append(this.user);
        buf.append(" Date: ").append(this.createDate);
        buf.append(" QueryId: ").append(this.queryId);
        buf.append(" Query: ").append(this.query);
        buf.append(" Host: ").append(this.host);
        buf.append(" SetupTime(ms): ").append(this.setupTime);
        buf.append(" PageTimes(ms): ").append(this.pageTimes);
        buf.append("\n");
        return buf.toString();
    }
    
    public static Schema<QueryMetric> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<QueryMetric> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<QueryMetric> SCHEMA = new Schema<QueryMetric>() {
        public QueryMetric newMessage() {
            return new QueryMetric();
        }
        
        public Class<QueryMetric> typeClass() {
            return QueryMetric.class;
        }
        
        public String messageName() {
            return QueryMetric.class.getSimpleName();
        }
        
        public String messageFullName() {
            return QueryMetric.class.getName();
        }
        
        public boolean isInitialized(QueryMetric message) {
            return true;
        }
        
        public void writeTo(Output output, QueryMetric message) throws IOException {
            if (message.queryType != null)
                output.writeString(1, message.queryType, false);
            
            if (message.user != null)
                output.writeString(2, message.user, false);
            
            if (message.createDate != null)
                output.writeInt64(3, message.createDate.getTime(), false);
            
            if (message.queryId != null)
                output.writeString(4, message.queryId, false);
            
            output.writeUInt64(5, message.setupTime, false);
            
            if (message.pageTimes != null) {
                for (PageMetric pageTimes : message.pageTimes) {
                    if (pageTimes != null)
                        output.writeObject(6, pageTimes, PageMetric.getSchema(), true);
                }
            }
            
            if (message.query != null)
                output.writeString(7, message.query, false);
            
            if (message.host != null)
                output.writeString(7, message.host, false);
            
        }
        
        public void mergeFrom(Input input, QueryMetric message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.queryType = input.readString();
                        break;
                    case 2:
                        message.user = input.readString();
                        break;
                    case 3:
                        message.createDate = new Date(input.readInt64());
                        break;
                    case 4:
                        message.queryId = input.readString();
                        break;
                    case 5:
                        message.setupTime = input.readUInt64();
                        break;
                    case 6:
                        if (message.pageTimes == null)
                            message.pageTimes = new ArrayList<>();
                        message.pageTimes.add(input.mergeObject(null, PageMetric.getSchema()));
                        break;
                    case 7:
                        message.query = input.readString();
                        break;
                    case 8:
                        message.host = input.readString();
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
                    return "queryType";
                case 2:
                    return "user";
                case 3:
                    return "createDate";
                case 4:
                    return "queryId";
                case 5:
                    return "setupTime";
                case 6:
                    return "pageTimes";
                case 7:
                    return "query";
                case 8:
                    return "host";
                default:
                    return null;
            }
        }
        
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }
        
        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<>();
        {
            fieldMap.put("queryType", 1);
            fieldMap.put("user", 2);
            fieldMap.put("createDate", 3);
            fieldMap.put("queryId", 4);
            fieldMap.put("setupTime", 5);
            fieldMap.put("pageTimes", 6);
            fieldMap.put("query", 7);
            fieldMap.put("host", 8);
        }
    };
}
