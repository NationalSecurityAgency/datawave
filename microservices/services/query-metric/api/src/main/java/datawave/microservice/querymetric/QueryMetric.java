package datawave.microservice.querymetric;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.time.DateUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;

import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl.Parameter;
import datawave.webservice.query.result.event.HasMarkings;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class QueryMetric extends BaseQueryMetric implements Serializable, Message<QueryMetric>, Comparable<QueryMetric>, HasMarkings {
    
    @Override
    public int compareTo(QueryMetric that) {
        return this.getCreateDate().compareTo(that.getCreateDate());
    }
    
    private static final long serialVersionUID = 1L;
    
    public QueryMetric() {
        this.createDate = DateUtils.truncate(new Date(), Calendar.SECOND);
        this.host = System.getProperty("jboss.host.name");
    }
    
    public QueryMetric(QueryMetric other) {
        this.queryType = other.queryType;
        this.user = other.user;
        this.userDN = other.userDN;
        if (other.createDate != null) {
            this.createDate = new Date(other.createDate.getTime());
        }
        this.queryId = other.queryId;
        this.setupTime = other.setupTime;
        this.query = other.query;
        this.host = other.host;
        this.createCallTime = other.createCallTime;
        if (other.pageTimes != null) {
            this.pageTimes = new ArrayList<>();
            for (PageMetric p : other.pageTimes) {
                this.pageTimes.add(p.duplicate());
            }
        }
        this.numPages = other.numPages;
        this.numResults = other.numResults;
        this.proxyServers = other.proxyServers;
        this.errorMessage = other.errorMessage;
        this.errorCode = other.errorCode;
        this.lifecycle = other.lifecycle;
        this.queryAuthorizations = other.queryAuthorizations;
        if (other.beginDate != null) {
            this.beginDate = new Date(other.beginDate.getTime());
        }
        if (other.endDate != null) {
            this.endDate = new Date(other.endDate.getTime());
        }
        if (other.positiveSelectors != null) {
            this.positiveSelectors = Lists.newArrayList(other.positiveSelectors);
        }
        if (other.negativeSelectors != null) {
            this.negativeSelectors = Lists.newArrayList(other.negativeSelectors);
        }
        if (other.lastUpdated != null) {
            this.lastUpdated = new Date(other.lastUpdated.getTime());
        }
        this.columnVisibility = other.columnVisibility;
        this.queryLogic = other.queryLogic;
        this.numUpdates = other.numUpdates;
        this.queryName = other.queryName;
        this.parameters = other.parameters;
        
        this.sourceCount = other.sourceCount;
        this.nextCount = other.nextCount;
        this.seekCount = other.seekCount;
        this.yieldCount = other.yieldCount;
        this.versionMap = other.versionMap;
        this.docSize = other.docSize;
        this.docRanges = other.docRanges;
        this.fiRanges = other.fiRanges;
        this.plan = other.plan;
        this.loginTime = other.loginTime;
        
        if (other.predictions != null) {
            this.predictions = new HashSet<>();
            for (Prediction p : other.predictions) {
                this.predictions.add(p.duplicate());
            }
        }
    }
    
    @Override
    public void populate(Query query) {
        setQueryType(query.getClass());
        setQueryId(query.getId().toString());
        setUser(query.getOwner());
        setUserDN(query.getUserDN());
        setQuery(query.getQuery());
        setQueryLogic(query.getQueryLogicName());
        setBeginDate(query.getBeginDate());
        setEndDate(query.getEndDate());
        setQueryAuthorizations(query.getQueryAuthorizations());
        setQueryName(query.getQueryName());
        setParameters(query.getParameters());
        setColumnVisibility(query.getColumnVisibility());
    }
    
    public BaseQueryMetric duplicate() {
        return new QueryMetric(this);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(this.getCreateDate()).append(this.getQueryId()).append(this.getQueryType())
                        .append(this.getQueryAuthorizations()).append(this.getColumnVisibility()).append(this.getBeginDate()).append(this.getEndDate())
                        .append(this.getSetupTime()).append(this.getUser()).append(this.getUserDN()).append(this.getQuery()).append(this.getQueryLogic())
                        .append(this.getHost()).append(this.getPageTimes()).append(this.getProxyServers()).append(this.getLifecycle())
                        .append(this.getErrorMessage()).append(this.getCreateCallTime()).append(this.getErrorCode()).append(this.getQueryName())
                        .append(this.getParameters()).append(this.getSourceCount()).append(this.getNextCount()).append(this.getSeekCount())
                        .append(this.getYieldCount()).append(this.getDocSize()).append(this.getDocRanges()).append(this.getFiRanges()).append(this.getPlan())
                        .append(this.getLoginTime()).append(this.getPredictions()).append(this.getMarkings()).append(this.getNumUpdates())
                        .append(this.getVersionMap()).toHashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof QueryMetric) {
            QueryMetric other = (QueryMetric) o;
            return new EqualsBuilder().append(this.getQueryId(), other.getQueryId()).append(this.getQueryType(), other.getQueryType())
                            .append(this.getQueryAuthorizations(), other.getQueryAuthorizations())
                            .append(this.getColumnVisibility(), other.getColumnVisibility()).append(this.getBeginDate(), other.getBeginDate())
                            .append(this.getEndDate(), other.getEndDate()).append(this.getCreateDate(), other.getCreateDate())
                            .append(this.getSetupTime(), other.getSetupTime()).append(this.getCreateCallTime(), other.getCreateCallTime())
                            .append(this.getUser(), other.getUser()).append(this.getUserDN(), other.getUserDN()).append(this.getQuery(), other.getQuery())
                            .append(this.getQueryLogic(), other.getQueryLogic()).append(this.getQueryName(), other.getQueryName())
                            .append(this.getParameters(), other.getParameters()).append(this.getHost(), other.getHost())
                            .append(this.getPageTimes(), other.getPageTimes()).append(this.getProxyServers(), other.getProxyServers())
                            .append(this.getLifecycle(), other.getLifecycle()).append(this.getErrorMessage(), other.getErrorMessage())
                            .append(this.getErrorCode(), other.getErrorCode()).append(this.getSourceCount(), other.getSourceCount())
                            .append(this.getNextCount(), other.getNextCount()).append(this.getSeekCount(), other.getSeekCount())
                            .append(this.getYieldCount(), other.getYieldCount()).append(this.getDocSize(), other.getDocSize())
                            .append(this.getDocRanges(), other.getDocRanges()).append(this.getFiRanges(), other.getFiRanges())
                            .append(this.getPlan(), other.getPlan()).append(this.getLoginTime(), other.getLoginTime())
                            .append(this.getPredictions(), other.getPredictions()).append(this.getMarkings(), other.getMarkings())
                            .append(this.getNumUpdates(), other.getNumUpdates()).append(this.getVersionMap(), other.getVersionMap()).isEquals();
        } else {
            return false;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Type: ").append(queryType);
        buf.append(" User: ").append(user);
        buf.append(" UserDN: ").append(userDN);
        buf.append(" Date: ").append(createDate);
        buf.append(" QueryId: ").append(queryId);
        buf.append(" Query: ").append(query);
        buf.append(" Query Plan: ").append(this.getPlan());
        buf.append(" Query Type: ").append(queryType);
        buf.append(" Query Logic: ").append(queryLogic);
        buf.append(" Query Name: ").append(queryName);
        buf.append(" Positive Selectors: ").append(this.getPositiveSelectors());
        buf.append(" Negative Selectors: ").append(this.getNegativeSelectors());
        buf.append(" Authorizations: ").append(queryAuthorizations);
        buf.append(" ColumnVisibility: ").append(this.columnVisibility);
        buf.append(" Begin Date: ").append(this.beginDate);
        buf.append(" End Date: ").append(this.endDate);
        buf.append(" Parameters: ").append(this.getParameters());
        buf.append(" Host: ").append(this.getHost());
        buf.append(" SetupTime(ms): ").append(this.getSetupTime());
        buf.append(" CreateCallTime(ms): ").append(this.getCreateCallTime());
        buf.append(" PageTimes(ms): ").append(this.getPageTimes());
        buf.append(" ProxyServers: ").append(this.getProxyServers());
        buf.append(" Lifecycle: ").append(this.getLifecycle());
        buf.append(" ErrorCode: ").append(this.getErrorCode());
        buf.append(" ErrorMessage: ").append(this.getErrorMessage());
        buf.append(" Source Count: ").append(this.getSourceCount());
        buf.append(" NextCount: ").append(this.getNextCount());
        buf.append(" Seek Count: ").append(this.getSeekCount());
        buf.append(" Yield Count: ").append(this.getYieldCount());
        buf.append(" Doc Size: ").append(this.getDocSize());
        buf.append(" Doc Ranges: ").append(this.getDocRanges());
        buf.append(" FI Ranges: ").append(this.getFiRanges());
        buf.append(" Login Time: ").append(this.getLoginTime());
        buf.append(" Predictions: ").append(this.getPredictions());
        buf.append(" Markings: ").append(this.getMarkings());
        buf.append(" NumUpdates: ").append(this.getNumUpdates());
        buf.append(" VersionMap: ").append(this.getVersionMap());
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
            if (message.queryType != null) {
                output.writeString(1, message.queryType, false);
            }
            
            if (message.user != null) {
                output.writeString(2, message.user, false);
            }
            
            if (message.createDate != null) {
                output.writeInt64(3, message.createDate.getTime(), false);
            }
            
            if (message.queryId != null) {
                output.writeString(4, message.queryId, false);
            }
            
            output.writeUInt64(5, message.setupTime, false);
            
            if (message.pageTimes != null) {
                for (PageMetric pageTimes : message.pageTimes) {
                    if (pageTimes != null) {
                        output.writeObject(6, pageTimes, PageMetric.getSchema(), true);
                    }
                }
            }
            
            if (message.query != null) {
                output.writeString(7, message.query, false);
            }
            
            if (message.host != null) {
                output.writeString(8, message.host, false);
            }
            
            if (message.proxyServers != null) {
                for (String h : message.proxyServers) {
                    if (h != null) {
                        output.writeString(9, h, true);
                    }
                }
            }
            
            if (message.lifecycle != null) {
                output.writeString(10, message.lifecycle.toString(), false);
            }
            
            if (message.errorMessage != null) {
                output.writeString(11, message.errorMessage, false);
            }
            
            if (message.queryAuthorizations != null) {
                output.writeString(12, message.queryAuthorizations, false);
            }
            
            if (message.beginDate != null) {
                output.writeInt64(13, message.beginDate.getTime(), false);
            }
            
            if (message.endDate != null) {
                output.writeInt64(14, message.endDate.getTime(), false);
            }
            
            if (message.createCallTime != -1) {
                output.writeInt64(15, message.createCallTime, false);
            }
            
            if (message.positiveSelectors != null) {
                for (String s : message.positiveSelectors) {
                    if (s != null) {
                        output.writeString(16, s, true);
                    }
                }
            }
            
            if (message.negativeSelectors != null) {
                for (String s : message.negativeSelectors) {
                    if (s != null) {
                        output.writeString(17, s, true);
                    }
                }
            }
            
            if (message.lastUpdated != null) {
                output.writeInt64(18, message.lastUpdated.getTime(), false);
            }
            
            if (message.columnVisibility != null) {
                output.writeString(19, message.columnVisibility, false);
            }
            
            if (message.queryLogic != null) {
                output.writeString(20, message.queryLogic, false);
            }
            
            if (message.queryLogic != null) {
                output.writeInt64(21, message.numUpdates, false);
            }
            
            if (message.userDN != null) {
                output.writeString(22, message.userDN, false);
            }
            
            if (message.userDN != null) {
                output.writeInt64(23, message.numResults, false);
            }
            
            if (message.userDN != null) {
                output.writeInt64(24, message.numPages, false);
            }
            if (message.errorCode != null) {
                output.writeString(25, message.errorCode, false);
            }
            if (message.queryName != null) {
                output.writeString(26, message.queryName, false);
            }
            
            if (message.parameters != null) {
                for (Parameter p : message.parameters) {
                    if (p != null) {
                        output.writeObject(27, p, Parameter.getSchema(), true);
                    }
                }
            }
            
            output.writeInt64(28, message.sourceCount, false);
            output.writeInt64(29, message.nextCount, false);
            output.writeInt64(30, message.seekCount, false);
            output.writeInt64(31, message.yieldCount, false);
            output.writeInt64(32, message.docRanges, false);
            output.writeInt64(33, message.fiRanges, false);
            
            if (message.plan != null) {
                output.writeString(34, message.plan, false);
            }
            
            if (message.loginTime != -1) {
                output.writeUInt64(35, message.loginTime, false);
            }
            
            if (message.predictions != null) {
                for (Prediction prediction : message.predictions) {
                    if (prediction != null) {
                        output.writeObject(36, prediction, Prediction.getSchema(), true);
                    }
                }
            }
            
            if (message.versionMap != null) {
                for (Map.Entry<String,String> entry : message.versionMap.entrySet()) {
                    output.writeString(38, StringUtils.join(Arrays.asList(entry.getKey(), entry.getValue()), "\0"), true);
                }
            }
            
            output.writeInt64(39, message.docSize, false);
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
                        if (message.pageTimes == null) {
                            message.pageTimes = new ArrayList<PageMetric>();
                        }
                        message.pageTimes.add(input.mergeObject(null, PageMetric.getSchema()));
                        break;
                    case 7:
                        message.query = input.readString();
                        break;
                    case 8:
                        message.host = input.readString();
                        break;
                    case 9:
                        if (message.proxyServers == null) {
                            message.proxyServers = new ArrayList<String>();
                        }
                        message.proxyServers.add(input.readString());
                        break;
                    case 10:
                        message.lifecycle = Lifecycle.valueOf(input.readString());
                        break;
                    case 11:
                        message.errorMessage = input.readString();
                        break;
                    case 12:
                        message.queryAuthorizations = input.readString();
                        break;
                    case 13:
                        message.beginDate = new Date(input.readInt64());
                        break;
                    case 14:
                        message.endDate = new Date(input.readInt64());
                        break;
                    case 15:
                        message.createCallTime = input.readInt64();
                        break;
                    case 16:
                        if (message.positiveSelectors == null) {
                            message.positiveSelectors = new ArrayList<String>();
                        }
                        message.positiveSelectors.add(input.readString());
                        break;
                    case 17:
                        if (message.negativeSelectors == null) {
                            message.negativeSelectors = new ArrayList<String>();
                        }
                        message.negativeSelectors.add(input.readString());
                        break;
                    case 18:
                        message.lastUpdated = new Date(input.readInt64());
                        break;
                    case 19:
                        message.columnVisibility = input.readString();
                        break;
                    case 20:
                        message.queryLogic = input.readString();
                        break;
                    case 21:
                        message.numUpdates = input.readInt64();
                        break;
                    case 22:
                        message.userDN = input.readString();
                        break;
                    case 23:
                        message.numResults = input.readInt64();
                        break;
                    case 24:
                        message.numPages = input.readInt64();
                        break;
                    case 25:
                        message.errorCode = input.readString();
                        break;
                    case 26:
                        message.queryName = input.readString();
                        break;
                    case 27:
                        if (message.parameters == null) {
                            message.parameters = new HashSet<>();
                        }
                        message.parameters.add(input.mergeObject(null, Parameter.getSchema()));
                        break;
                    case 28:
                        message.sourceCount = input.readInt64();
                        break;
                    case 29:
                        message.nextCount = input.readInt64();
                        break;
                    case 30:
                        message.seekCount = input.readInt64();
                        break;
                    case 31:
                        message.yieldCount = input.readInt64();
                        break;
                    case 32:
                        message.docRanges = input.readInt64();
                        break;
                    case 33:
                        message.fiRanges = input.readInt64();
                        break;
                    case 34:
                        message.plan = input.readString();
                        break;
                    case 35:
                        message.loginTime = input.readUInt64();
                        break;
                    case 36:
                        if (message.predictions == null) {
                            message.predictions = new HashSet<>();
                        }
                        message.predictions.add(input.mergeObject(null, Prediction.getSchema()));
                        break;
                    case 37:
                        if (message.versionMap == null) {
                            message.versionMap = new TreeMap<>();
                        }
                        message.versionMap.put(DATAWAVE, input.readString());
                        break;
                    case 38:
                        if (message.versionMap == null) {
                            message.versionMap = new TreeMap<>();
                        }
                        String encoded = input.readString();
                        String[] split = StringUtils.split(encoded, "\0");
                        if (split.length == 2) {
                            message.versionMap.put(split[0], split[1]);
                        }
                        break;
                    case 39:
                        message.docSize = input.readInt64();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
            message.numPages = message.pageTimes.size();
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
                case 9:
                    return "proxyServers";
                case 10:
                    return "lifecycle";
                case 11:
                    return "errorMessage";
                case 12:
                    return "queryAuthorizations";
                case 13:
                    return "beginDate";
                case 14:
                    return "endDate";
                case 15:
                    return "createCallTime";
                case 16:
                    return "positiveSelectors";
                case 17:
                    return "negativeSelectors";
                case 18:
                    return "lastUpdated";
                case 19:
                    return "columnVisibility";
                case 20:
                    return "queryLogic";
                case 21:
                    return "numUpdates";
                case 22:
                    return "userDN";
                case 23:
                    return "numResults";
                case 24:
                    return "numPages";
                case 25:
                    return "errorCode";
                case 26:
                    return "queryName";
                case 27:
                    return "parameters";
                case 28:
                    return "sourceCount";
                case 29:
                    return "nextCount";
                case 30:
                    return "seekCount";
                case 31:
                    return "yieldCount";
                case 32:
                    return "docRanges";
                case 33:
                    return "fiRanges";
                case 34:
                    return "plan";
                case 35:
                    return "loginTime";
                case 36:
                    return "predictions";
                case 37:
                    return "version";
                case 38:
                    return "versionMap";
                case 39:
                    return "docSize";
                default:
                    return null;
            }
        }
        
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }
        
        final HashMap<String,Integer> fieldMap = new HashMap<>();
        
        {
            fieldMap.put("queryType", 1);
            fieldMap.put("user", 2);
            fieldMap.put("createDate", 3);
            fieldMap.put("queryId", 4);
            fieldMap.put("setupTime", 5);
            fieldMap.put("pageTimes", 6);
            fieldMap.put("query", 7);
            fieldMap.put("host", 8);
            fieldMap.put("proxyServers", 9);
            fieldMap.put("lifecycle", 10);
            fieldMap.put("errorMessage", 11);
            fieldMap.put("queryAuthorizations", 12);
            fieldMap.put("beginDate", 13);
            fieldMap.put("endDate", 14);
            fieldMap.put("createCallTime", 15);
            fieldMap.put("positiveSelectors", 16);
            fieldMap.put("negativeSelectors", 17);
            fieldMap.put("lastUpdated", 18);
            fieldMap.put("columnVisibility", 19);
            fieldMap.put("queryLogic", 20);
            fieldMap.put("numUpdates", 21);
            fieldMap.put("userDN", 22);
            fieldMap.put("numResults", 23);
            fieldMap.put("numPages", 24);
            fieldMap.put("errorCode", 25);
            fieldMap.put("queryName", 26);
            fieldMap.put("parameters", 27);
            fieldMap.put("sourceCount", 28);
            fieldMap.put("nextCount", 29);
            fieldMap.put("seekCount", 30);
            fieldMap.put("yieldCount", 31);
            fieldMap.put("docRanges", 32);
            fieldMap.put("fiRanges", 33);
            fieldMap.put("plan", 34);
            fieldMap.put("loginTime", 35);
            fieldMap.put("predictions", 36);
            fieldMap.put("version", 37);
            fieldMap.put("versionMap", 38);
            fieldMap.put("docSize", 39);
        }
    };
    
    @JsonIgnore
    public Schema<? extends BaseQueryMetric> getSchemaInstance() {
        return getSchema();
    }
}
