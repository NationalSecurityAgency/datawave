package datawave.webservice.query.metric;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import datawave.marking.MarkingFunctions;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.HasMarkings;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.log4j.Logger;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class BaseQueryMetric implements HasMarkings, Serializable {
    
    private static final Logger log = Logger.getLogger(BaseQueryMetric.class);
    
    @XmlAccessorType(XmlAccessType.NONE)
    public static class PageMetric implements Serializable, Message<PageMetric> {
        
        private static final long serialVersionUID = 1L;
        @XmlElement
        private long pagesize = 0;
        @XmlElement
        private long returnTime = 0;
        @XmlElement
        private long callTime = -1;
        @XmlElement
        private long serializationTime = -1;
        @XmlElement
        private long bytesWritten = -1;
        @XmlElement
        private long pageRequested = 0;
        @XmlElement
        private long pageReturned = 0;
        @XmlElement
        private long pageNumber = -1;
        @XmlElement
        private long loginTime = -1;
        
        public PageMetric() {
            super();
        }
        
        public PageMetric(long pagesize, long returnTime, long pageRequested, long pageReturned) {
            super();
            this.pagesize = pagesize;
            this.returnTime = returnTime;
            this.pageRequested = pageRequested;
            this.pageReturned = pageReturned;
        }
        
        public PageMetric(long pagesize, long returnTime, long callTime, long serializationTime, long bytesWritten, long pageRequested, long pageReturned) {
            this.pagesize = pagesize;
            this.callTime = callTime;
            this.serializationTime = serializationTime;
            this.bytesWritten = bytesWritten;
            this.returnTime = returnTime;
            this.pageRequested = pageRequested;
            this.pageReturned = pageReturned;
        }
        
        public PageMetric(long pagesize, long returnTime, long callTime, long serializationTime, long bytesWritten, long pageRequested, long pageReturned,
                        long loginTime) {
            this.pagesize = pagesize;
            this.callTime = callTime;
            this.serializationTime = serializationTime;
            this.bytesWritten = bytesWritten;
            this.returnTime = returnTime;
            this.pageRequested = pageRequested;
            this.pageReturned = pageReturned;
            this.loginTime = loginTime;
        }
        
        public PageMetric(PageMetric o) {
            super();
            this.pagesize = o.pagesize;
            this.returnTime = o.returnTime;
            this.callTime = o.callTime;
            this.serializationTime = o.serializationTime;
            this.bytesWritten = o.bytesWritten;
            this.pageRequested = o.pageRequested;
            this.pageReturned = o.pageReturned;
            this.pageNumber = o.pageNumber;
            this.loginTime = o.loginTime;
        }
        
        public PageMetric duplicate() {
            return new PageMetric(this);
        }
        
        public long getPagesize() {
            return pagesize;
        }
        
        public long getReturnTime() {
            return returnTime;
        }
        
        public long getCallTime() {
            return callTime;
        }
        
        public long getSerializationTime() {
            return serializationTime;
        }
        
        public long getBytesWritten() {
            return bytesWritten;
        }
        
        public void setPagesize(long pagesize) {
            this.pagesize = pagesize;
        }
        
        public void setReturnTime(long returnTime) {
            this.returnTime = returnTime;
        }
        
        public void setCallTime(long callTime) {
            this.callTime = callTime;
        }
        
        public void setSerializationTime(long serializationTime) {
            this.serializationTime = serializationTime;
        }
        
        public void setBytesWritten(long bytesWritten) {
            this.bytesWritten = bytesWritten;
        }
        
        public long getPageRequested() {
            return pageRequested;
        }
        
        public void setPageRequested(long pageRequested) {
            this.pageRequested = pageRequested;
        }
        
        public long getPageReturned() {
            return pageReturned;
        }
        
        public void setPageReturned(long pageReturned) {
            this.pageReturned = pageReturned;
        }
        
        public long getPageNumber() {
            return pageNumber;
        }
        
        public void setPageNumber(long pageNumber) {
            this.pageNumber = pageNumber;
        }
        
        public long getLoginTime() {
            return loginTime;
        }
        
        public void setLoginTime(long loginTime) {
            this.loginTime = loginTime;
        }
        
        public String toEventString() {
            StringBuilder sb = new StringBuilder();
            sb.append(pagesize).append("/");
            sb.append(returnTime).append("/");
            sb.append(callTime).append("/");
            sb.append(serializationTime).append("/");
            sb.append(bytesWritten).append("/");
            sb.append(pageRequested).append("/");
            sb.append(pageReturned).append("/");
            sb.append(loginTime);
            return sb.toString();
        }
        
        static public PageMetric parse(String s) {
            String[] parts = StringUtils.split(s, "/");
            PageMetric pageMetric = null;
            
            if (parts.length == 10) {
                // host/pageUuid/pageSize/returnTime/callTime/serializationTime/bytesWritten/pageRequested/pageReturned/loginTime
                pageMetric = new PageMetric(Long.parseLong(parts[2]), Long.parseLong(parts[3]), Long.parseLong(parts[4]), Long.parseLong(parts[5]),
                                Long.parseLong(parts[6]), Long.parseLong(parts[7]), Long.parseLong(parts[8]), Long.parseLong(parts[9]));
            } else if (parts.length == 9) {
                // /pageUuid/pageSize/returnTime/callTime/serializationTime/bytesWritten/pageRequested/pageReturned/loginTime
                pageMetric = new PageMetric(Long.parseLong(parts[1]), Long.parseLong(parts[2]), Long.parseLong(parts[3]), Long.parseLong(parts[4]),
                                Long.parseLong(parts[5]), Long.parseLong(parts[6]), Long.parseLong(parts[7]), Long.parseLong(parts[8]));
            } else if (parts.length == 8) {
                // pageSize/returnTime/callTime/serializationTime/bytesWritten/pageRequested/pageReturned/loginTime
                pageMetric = new PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]), Long.parseLong(parts[3]),
                                Long.parseLong(parts[4]), Long.parseLong(parts[5]), Long.parseLong(parts[6]), Long.parseLong(parts[7]));
            } else if (parts.length == 7) {
                // pageSize/returnTime/callTime/serializationTime/bytesWritten/pageRequested/pageReturned
                pageMetric = new PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]), Long.parseLong(parts[3]),
                                Long.parseLong(parts[4]), Long.parseLong(parts[5]), Long.parseLong(parts[6]));
            } else if (parts.length == 5) {
                // pageSize/returnTime/callTime/serializationTime/bytesWritten
                pageMetric = new PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]), Long.parseLong(parts[3]),
                                Long.parseLong(parts[4]), 0l, 0l);
            } else if (parts.length == 2) {
                // pageSize/returnTime
                pageMetric = new PageMetric(Long.parseLong(parts[0]), Long.parseLong(parts[1]), 0l, 0l);
            }
            return pageMetric;
        }
        
        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(pagesize).append(returnTime).append(callTime).append(serializationTime).append(bytesWritten)
                            .append(pageRequested).append(pageReturned).append(pageNumber).append(loginTime).toHashCode();
        }
        
        @Override
        public boolean equals(Object o) {
            if (null == o) {
                return false;
            }
            if (this == o) {
                return true;
            }
            if (o instanceof PageMetric) {
                PageMetric other = (PageMetric) o;
                return new EqualsBuilder().append(this.pagesize, other.pagesize).append(this.returnTime, other.returnTime)
                                .append(this.callTime, other.callTime).append(this.serializationTime, other.serializationTime)
                                .append(this.bytesWritten, other.bytesWritten).append(this.pageRequested, other.pageRequested)
                                .append(this.pageReturned, other.pageReturned).append(this.pageNumber, other.pageNumber)
                                .append(this.loginTime, other.loginTime).isEquals();
            } else {
                return false;
            }
        }
        
        @Override
        public String toString() {
            return new StringBuilder().append("Page number: ").append(this.pageNumber).append(" Requested: ").append(this.pageRequested).append(" Returned: ")
                            .append(this.pageReturned).append(" Pagesize: ").append(this.pagesize).append(" ReturnTime(ms): ").append(this.returnTime)
                            .append(" CallTime(ms): ").append(this.callTime).append(" SerializationTime(ms): ").append(this.serializationTime)
                            .append(" BytesWritten: ").append(this.bytesWritten).append(" LoginTime(ms): ").append(this.loginTime).toString();
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
                if (message.callTime != -1) {
                    output.writeUInt64(3, message.callTime, false);
                }
                if (message.serializationTime != -1) {
                    output.writeUInt64(4, message.serializationTime, false);
                }
                if (message.bytesWritten != -1) {
                    output.writeUInt64(5, message.bytesWritten, false);
                }
                if (message.pageRequested != -1) {
                    output.writeUInt64(6, message.pageRequested, false);
                }
                if (message.pageReturned != -1) {
                    output.writeUInt64(7, message.pageReturned, false);
                }
                if (message.pageNumber != -1) {
                    output.writeUInt64(8, message.pageNumber, false);
                }
                if (message.loginTime != -1) {
                    output.writeUInt64(9, message.loginTime, false);
                }
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
                        case 3:
                            message.callTime = input.readUInt64();
                            break;
                        case 4:
                            message.serializationTime = input.readUInt64();
                            break;
                        case 5:
                            message.bytesWritten = input.readUInt64();
                            break;
                        case 6:
                            message.pageRequested = input.readUInt64();
                            break;
                        case 7:
                            message.pageReturned = input.readUInt64();
                            break;
                        case 8:
                            message.pageNumber = input.readUInt64();
                            break;
                        case 9:
                            message.loginTime = input.readUInt64();
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
                    case 3:
                        return "callTime";
                    case 4:
                        return "serializationTime";
                    case 5:
                        return "bytesWritten";
                    case 6:
                        return "pageRequested";
                    case 7:
                        return "pageReturned";
                    case 8:
                        return "pageNumber";
                    case 9:
                        return "loginTime";
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
                fieldMap.put("pagesize", 1);
                fieldMap.put("returnTime", 2);
                fieldMap.put("callTime", 3);
                fieldMap.put("serializationTime", 4);
                fieldMap.put("bytesWritten", 5);
                fieldMap.put("pageRequested", 6);
                fieldMap.put("pageReturned", 7);
                fieldMap.put("pageNumber", 8);
                fieldMap.put("loginTime", 9);
            }
        };
    }
    
    @XmlAccessorType(XmlAccessType.NONE)
    public static class Prediction implements Serializable, Comparable<Prediction>, Message<Prediction> {
        
        private static final long serialVersionUID = 1L;
        
        // The name of the prediction
        @XmlElement
        private String name = null;
        
        // The predicted value
        @XmlElement
        private double prediction = 0;
        
        public Prediction() {
            super();
        }
        
        public Prediction(String name, double value) {
            super();
            this.name = name;
            this.prediction = value;
        }
        
        public Prediction(Prediction o) {
            super();
            this.name = o.name;
            this.prediction = o.prediction;
        }
        
        public Prediction duplicate() {
            return new Prediction(this);
        }
        
        public String getName() {
            return name;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public double getPrediction() {
            return prediction;
        }
        
        public void setPrediction(double prediction) {
            this.prediction = prediction;
        }
        
        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(name).append(prediction).toHashCode();
        }
        
        @Override
        public boolean equals(Object o) {
            if (null == o) {
                return false;
            }
            if (this == o) {
                return true;
            }
            if (o instanceof Prediction) {
                Prediction other = (Prediction) o;
                return new EqualsBuilder().append(this.name, other.name).append(this.prediction, other.prediction).isEquals();
            } else {
                return false;
            }
        }
        
        @Override
        public int compareTo(Prediction o) {
            return new CompareToBuilder().append(name, o.name).append(prediction, o.prediction).toComparison();
        }
        
        @Override
        public String toString() {
            return new StringBuilder().append("Name: ").append(this.name).append(" Prediction: ").append(this.prediction).toString();
        }
        
        public static Schema<Prediction> getSchema() {
            return SCHEMA;
        }
        
        @Override
        public Schema<Prediction> cachedSchema() {
            return SCHEMA;
        }
        
        private static final Schema<Prediction> SCHEMA = new Schema<Prediction>() {
            public Prediction newMessage() {
                return new Prediction();
            }
            
            public Class<Prediction> typeClass() {
                return Prediction.class;
            }
            
            public String messageName() {
                return Prediction.class.getSimpleName();
            }
            
            public String messageFullName() {
                return Prediction.class.getName();
            }
            
            public boolean isInitialized(Prediction message) {
                return true;
            }
            
            public void writeTo(Output output, Prediction message) throws IOException {
                output.writeString(1, message.name, false);
                output.writeDouble(2, message.prediction, false);
            }
            
            public void mergeFrom(Input input, Prediction message) throws IOException {
                int number;
                while ((number = input.readFieldNumber(this)) != 0) {
                    switch (number) {
                        case 1:
                            message.name = input.readString();
                            break;
                        case 2:
                            message.prediction = input.readDouble();
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
                        return "name";
                    case 2:
                        return "prediction";
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
                fieldMap.put("name", 1);
                fieldMap.put("prediction", 2);
            }
        };
        
    }
    
    @XmlElement
    protected String queryType = null;
    @XmlElement
    protected String user = null;
    @XmlElement
    protected String userDN = null;
    @XmlElement
    protected Date createDate = null;
    @XmlElement
    protected String queryId = null;
    @XmlElement
    protected long setupTime = 0;
    @XmlElement
    protected String query = null;
    @XmlElement
    protected String host = null;
    @XmlElement
    protected long createCallTime = -1;
    @XmlElementWrapper(name = "pageMetrics")
    @XmlElement(name = "pageMetric")
    protected ArrayList<PageMetric> pageTimes = new ArrayList<PageMetric>();
    @XmlElement
    protected Collection<String> proxyServers = null;
    @XmlElement
    protected String errorMessage = null;
    @XmlElement
    protected String errorCode = null;
    @XmlElement
    protected Lifecycle lifecycle = Lifecycle.NONE;
    @XmlElement
    protected String queryAuthorizations = null;
    @XmlElement
    protected Date beginDate = null;
    @XmlElement
    protected Date endDate = null;
    @XmlElementWrapper(name = "positiveSelectors")
    @XmlElement(name = "positiveSelector")
    protected List<String> positiveSelectors = null;
    @XmlElementWrapper(name = "negativeSelectors")
    @XmlElement(name = "negativeSelector")
    protected List<String> negativeSelectors = null;
    @XmlElement
    protected Date lastUpdated = null;
    @XmlElement
    protected String columnVisibility = null;
    @XmlElement
    protected String queryLogic = null;
    @XmlElement
    protected long numPages = 0;
    @XmlElement
    protected long numResults = 0;
    @XmlElement
    protected String queryName = null;
    @XmlElement
    protected Set<Parameter> parameters = new HashSet<Parameter>();
    @XmlElement
    protected long sourceCount = 0;
    @XmlElement
    protected long nextCount = 0;
    @XmlElement
    protected long seekCount = 0;
    @XmlElement
    protected long yieldCount = 0L;
    @XmlElement
    protected String version = getVersion();
    @XmlElement
    protected long docRanges = 0;
    @XmlElement
    protected long fiRanges = 0;
    @XmlElement
    protected String plan = null;
    @XmlElement
    protected long loginTime = -1;
    @XmlElementWrapper(name = "predictions")
    @XmlElement(name = "prediction")
    protected Set<Prediction> predictions = new HashSet<Prediction>();
    
    protected int lastWrittenHash = 0;
    protected long numUpdates = 0;
    
    public enum Lifecycle {
        
        NONE, DEFINED, INITIALIZED, RESULTS, CLOSED, CANCELLED, MAXRESULTS, NEXTTIMEOUT, TIMEOUT, SHUTDOWN, MAXWORK
    }
    
    public String getQueryType() {
        return queryType;
    }
    
    public String getUser() {
        return user;
    }
    
    public Date getCreateDate() {
        if (this.createDate != null) {
            return (Date) createDate.clone();
        } else {
            return null;
        }
    }
    
    public void setCreateDate(Date date) {
        this.createDate = date;
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    public long getSetupTime() {
        return setupTime;
    }
    
    public long getCreateCallTime() {
        return createCallTime;
    }
    
    public String getQuery() {
        return query;
    }
    
    public String getPlan() {
        return plan;
    }
    
    public String getHost() {
        return host;
    }
    
    public List<PageMetric> getPageTimes() {
        return pageTimes;
    }
    
    public long getNumResults() {
        return numResults;
    }
    
    public long getNumPages() {
        return numPages;
    }
    
    @JsonIgnore
    @XmlElement(name = "elapsedTime")
    public long getElapsedTime() {
        if (lastUpdated != null && createDate != null) {
            return lastUpdated.getTime() - createDate.getTime();
        } else {
            return 0;
        }
    }
    
    public String getQueryName() {
        return queryName;
    }
    
    public Set<Parameter> getParameters() {
        return parameters;
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
    
    public void setCreateCallTime(long createCallTime) {
        this.createCallTime = createCallTime;
    }
    
    public void setQuery(String query) {
        this.query = query;
    }
    
    public void setPlan(String plan) {
        this.plan = plan;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public String getVersion() {
        String returnStr = "";
        try {
            final Properties props = new Properties();
            InputStream in = BaseQueryMetric.class.getResourceAsStream("/version.properties");
            if (in != null) {
                props.load(in);
                returnStr = props.getProperty("currentVersion");
                in.close();
            } else {
                log.warn("version.properties InputStream is null. Keeping version string empty.");
            }
            
        } catch (IOException e) {
            log.warn("IOException encountered, attempting to read in version.properties.");
        }
        return returnStr;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }
    
    public void addPageTime(long pagesize, long timeToReturn, long requestedTime, long returnedTime) {
        this.numPages++;
        this.numResults += pagesize;
        PageMetric pageMetric = new PageMetric(pagesize, timeToReturn, requestedTime, returnedTime);
        pageMetric.setPageNumber(this.numPages);
        this.pageTimes.add(pageMetric);
    }
    
    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }
    
    public void setParameters(Set<Parameter> parameters) {
        this.parameters = parameters;
    }
    
    public long getSourceCount() {
        return sourceCount;
    }
    
    public void setSourceCount(long sourceCount) {
        this.sourceCount = sourceCount;
    }
    
    public long getNextCount() {
        return nextCount;
    }
    
    public void setNextCount(long nextCount) {
        this.nextCount = nextCount;
    }
    
    public long getSeekCount() {
        return seekCount;
    }
    
    public void setSeekCount(long seekCount) {
        this.seekCount = seekCount;
    }
    
    public long getYieldCount() {
        return this.yieldCount;
    }
    
    public void setYieldCount(long yieldCount) {
        this.yieldCount = yieldCount;
    }
    
    public long getDocRanges() {
        return docRanges;
    }
    
    public void setDocRanges(long docRanges) {
        this.docRanges = docRanges;
    }
    
    public long getFiRanges() {
        return fiRanges;
    }
    
    public void setFiRanges(long fiRanges) {
        this.fiRanges = fiRanges;
    }
    
    public long getLoginTime() {
        return loginTime;
    }
    
    public void setLoginTime(long loginTime) {
        this.loginTime = loginTime;
    }
    
    public void addPageMetric(PageMetric pageMetric) {
        this.numPages++;
        this.numResults += pageMetric.getPagesize();
        pageMetric.setPageNumber(this.numPages);
        this.pageTimes.add(pageMetric);
    }
    
    public Set<Prediction> getPredictions() {
        return this.predictions;
    }
    
    public void addPrediction(Prediction prediction) {
        this.predictions.add(prediction);
    }
    
    public void setError(Throwable t) {
        if (t.getCause() instanceof QueryException) {
            QueryException qe = (QueryException) t.getCause();
            this.setErrorCode(qe.getErrorCode());
            this.setErrorMessage(qe.getMessage());
        } else {
            this.setErrorMessage(t.getCause() != null ? t.getCause().getMessage() : t.getMessage());
        }
    }
    
    public Collection<String> getProxyServers() {
        return proxyServers;
    }
    
    public void setProxyServers(Collection<String> proxyServers) {
        this.proxyServers = proxyServers;
    }
    
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    public Lifecycle getLifecycle() {
        return lifecycle;
    }
    
    public void setLifecycle(Lifecycle lifecycle) {
        if (!this.isLifecycleFinal()) {
            this.lifecycle = lifecycle;
        }
    }
    
    /**
     * determines whether or not lifecycle is a final status
     *
     * @return true if lifecycle represents a final status, false otherwise
     */
    @JsonIgnore
    public boolean isLifecycleFinal() {
        return Lifecycle.CLOSED == lifecycle || Lifecycle.CANCELLED == lifecycle || Lifecycle.MAXRESULTS == lifecycle || Lifecycle.NEXTTIMEOUT == lifecycle
                        || Lifecycle.TIMEOUT == lifecycle || Lifecycle.SHUTDOWN == lifecycle;
    }
    
    public int getLastWrittenHash() {
        return lastWrittenHash;
    }
    
    public void setLastWrittenHash(int lastWrittenHash) {
        this.lastWrittenHash = lastWrittenHash;
    }
    
    public String getQueryAuthorizations() {
        return queryAuthorizations;
    }
    
    public void setQueryAuthorizations(String auths) {
        this.queryAuthorizations = auths;
    }
    
    public Date getBeginDate() {
        return beginDate;
    }
    
    public Date getEndDate() {
        return endDate;
    }
    
    public void setBeginDate(Date beginDate) {
        this.beginDate = beginDate;
    }
    
    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }
    
    public List<String> getNegativeSelectors() {
        return negativeSelectors;
    }
    
    public void setNegativeSelectors(List<String> negativeSelectors) {
        this.negativeSelectors = negativeSelectors;
    }
    
    public List<String> getPositiveSelectors() {
        return positiveSelectors;
    }
    
    public void setPositiveSelectors(List<String> positiveSelectors) {
        this.positiveSelectors = positiveSelectors;
    }
    
    public Date getLastUpdated() {
        if (lastUpdated != null) {
            return (Date) lastUpdated.clone();
        } else {
            return null;
        }
    }
    
    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public String getColumnVisibility() {
        return columnVisibility;
    }
    
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
    
    public String getQueryLogic() {
        return queryLogic;
    }
    
    public void setQueryLogic(String queryLogic) {
        this.queryLogic = queryLogic;
    }
    
    public long getNumUpdates() {
        return numUpdates;
    }
    
    public void setNumUpdates(long numUpdates) {
        this.numUpdates = numUpdates;
    }
    
    public String getUserDN() {
        return userDN;
    }
    
    public void setUserDN(String userDN) {
        this.userDN = userDN;
    }
    
    public void setPageTimes(ArrayList<PageMetric> pageTimes) {
        this.pageTimes = pageTimes;
        this.numResults = 0;
        if (pageTimes != null) {
            this.numPages = pageTimes.size();
            pageTimes.forEach(p -> this.numResults += p.getPagesize());
        }
    }
    
    public void setPredictions(Set<Prediction> predictions) {
        this.predictions = predictions;
    }
    
    @Override
    public void setMarkings(Map<String,String> markings) {
        if (markings == null || markings.isEmpty()) {
            this.columnVisibility = null;
        } else {
            this.columnVisibility = markings.get(MarkingFunctions.Default.COLUMN_VISIBILITY);
        }
    }
    
    @Override
    public Map<String,String> getMarkings() {
        Map<String,String> markings = new HashMap<>();
        markings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, this.columnVisibility);
        return markings;
    }
    
    public Schema<? extends BaseQueryMetric> getSchemaInstance() {
        return null;
    }
    
    public BaseQueryMetric duplicate() {
        // No op here
        return null;
    }
    
}
