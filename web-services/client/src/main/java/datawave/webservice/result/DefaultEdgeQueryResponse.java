package datawave.webservice.result;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.DefaultEdge;
import datawave.webservice.query.result.edge.EdgeBase;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

/**
 * A response object for holding data returned from edge table queries.
 */
@XmlRootElement(name = "DefaultEdgeQueryResponse")
@XmlAccessorType(XmlAccessType.NONE)
public class DefaultEdgeQueryResponse extends EdgeQueryResponseBase implements Serializable, TotalResultsAware, Message<DefaultEdgeQueryResponse> {
    private static final long serialVersionUID = -8080688956850811620L;
    
    @XmlElement(name = "SecurityMarkings", nillable = false)
    private String securityMarkings;
    
    @XmlElement(name = "TotalEvents")
    private Long totalEvents = null;
    
    @XmlElementWrapper(name = "Edges")
    @XmlElement(name = "Edge")
    private List<DefaultEdge> edges = null;
    
    public DefaultEdgeQueryResponse() {}
    
    public DefaultEdgeQueryResponse(String securityMarkings, List<DefaultEdge> edges) {
        this.securityMarkings = securityMarkings;
        this.edges = new ArrayList<DefaultEdge>(edges);
    }
    
    public String getSecurityMarkings() {
        return securityMarkings;
    }
    
    public void setMarkings(Map<String,String> markings) {
        this.markings = markings;
        this.setSecurityMarkings(markings.get("security"));
    }
    
    public Map<String,String> getMarkings() {
        return markings;
    }
    
    public void setSecurityMarkings(String securityMarkings) {
        this.securityMarkings = securityMarkings;
    }
    
    @Override
    public void addEdge(EdgeBase edge) {
        if (edges == null)
            edges = new ArrayList<DefaultEdge>();
        edges.add((DefaultEdge) edge);
    }

    @Override
    public void setEdges(List<EdgeBase> entries) {
        if (entries == null || entries.isEmpty()) {
            this.edges = null;
        } else {
            List<DefaultEdge> edges = new ArrayList<>(entries.size());
            for (EdgeBase edge : entries) {
                edges.add((DefaultEdge) edge);
            }
            this.edges = edges;
        }
    }

    @Override
    public List<? extends EdgeBase> getEdges() {
        return Collections.unmodifiableList(edges);
    }
    
    public static Schema<DefaultEdgeQueryResponse> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<DefaultEdgeQueryResponse> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<DefaultEdgeQueryResponse> SCHEMA = new Schema<DefaultEdgeQueryResponse>() {
        // schema methods
        
        public DefaultEdgeQueryResponse newMessage() {
            return new DefaultEdgeQueryResponse();
        }
        
        public Class<DefaultEdgeQueryResponse> typeClass() {
            return DefaultEdgeQueryResponse.class;
        }
        
        public String messageName() {
            return DefaultEdgeQueryResponse.class.getSimpleName();
        }
        
        public String messageFullName() {
            return DefaultEdgeQueryResponse.class.getName();
        }
        
        public boolean isInitialized(DefaultEdgeQueryResponse message) {
            return true;
        }
        
        public void writeTo(Output output, DefaultEdgeQueryResponse message) throws IOException {
            
            if (message.getQueryId() != null) {
                output.writeString(1, message.getQueryId(), false);
            }
            
            if (message.getLogicName() != null) {
                output.writeString(2, message.getLogicName(), false);
            }
            
            output.writeUInt64(3, message.getOperationTimeMS(), false);
            
            List<String> messages = message.getMessages();
            if (messages != null) {
                for (String msg : messages) {
                    if (msg != null)
                        output.writeString(4, msg, true);
                }
            }
            
            List<QueryExceptionType> exceptions = message.getExceptions();
            if (exceptions != null) {
                for (QueryExceptionType exception : exceptions) {
                    if (exception != null)
                        output.writeObject(5, exception, QueryExceptionType.getSchema(), true);
                }
            }
            
            if (message.securityMarkings != null)
                output.writeString(6, message.securityMarkings, false);
            
            if (message.edges != null) {
                for (DefaultEdge edge : message.edges) {
                    if (edge != null)
                        output.writeObject(7, edge, DefaultEdge.getSchema(), true);
                }
            }
        }
        
        public void mergeFrom(Input input, DefaultEdgeQueryResponse message) throws IOException {
            LinkedList<QueryExceptionType> exceptions = null;
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setQueryId(input.readString());
                        break;
                    case 2:
                        message.setLogicName(input.readString());
                        break;
                    case 3:
                        message.setOperationTimeMS(input.readUInt64());
                        break;
                    case 4:
                        message.addMessage(input.readString());
                        break;
                    case 5:
                        if (exceptions == null)
                            exceptions = new LinkedList<QueryExceptionType>();
                        exceptions.add(input.mergeObject(null, QueryExceptionType.getSchema()));
                        break;
                    case 6:
                        message.securityMarkings = input.readString();
                        break;
                    case 7:
                        if (message.edges == null)
                            message.edges = new ArrayList<DefaultEdge>();
                        message.edges.add(input.mergeObject(null, DefaultEdge.getSchema()));
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
            if (exceptions != null)
                message.setExceptions(exceptions);
        }
        
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "queryId";
                case 2:
                    return "logicName";
                case 3:
                    return "operationTimeMs";
                case 4:
                    return "messages";
                case 5:
                    return "exceptions";
                case 6:
                    return "securityMarkings";
                case 7:
                    return "edges";
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
            fieldMap.put("queryId", 1);
            fieldMap.put("logicName", 2);
            fieldMap.put("operationTimeMs", 3);
            fieldMap.put("messages", 4);
            fieldMap.put("exceptions", 5);
            fieldMap.put("securityMarkings", 6);
            fieldMap.put("edges", 7);
        }
    };
    
    @Override
    public void setTotalResults(long totalResults) {
        this.totalEvents = totalResults;
    }
    
    @Override
    public long getTotalResults() {
        return totalEvents == null ? -1 : totalEvents;
    }
}
