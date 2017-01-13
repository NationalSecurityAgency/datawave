package nsa.datawave.webservice.query.metric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import nsa.datawave.webservice.HtmlProvider;
import nsa.datawave.webservice.query.exception.QueryExceptionType;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "QueryMetricListResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryMetricListResponse extends BaseQueryMetricListResponse<QueryMetric> implements Message<QueryMetricListResponse>, HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    
    public QueryMetricListResponse() {
        super();
    }
    
    public static Schema<QueryMetricListResponse> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<QueryMetricListResponse> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<QueryMetricListResponse> SCHEMA = new Schema<QueryMetricListResponse>() {
        // schema methods
        
        public QueryMetricListResponse newMessage() {
            return new QueryMetricListResponse();
        }
        
        public Class<QueryMetricListResponse> typeClass() {
            return QueryMetricListResponse.class;
        }
        
        public String messageName() {
            return QueryMetricListResponse.class.getSimpleName();
        }
        
        public String messageFullName() {
            return QueryMetricListResponse.class.getName();
        }
        
        public boolean isInitialized(QueryMetricListResponse message) {
            return true;
        }
        
        public void writeTo(Output output, QueryMetricListResponse message) throws IOException {
            output.writeUInt64(1, message.getOperationTimeMS(), false);
            
            List<String> messages = message.getMessages();
            if (messages != null) {
                for (String msg : messages) {
                    if (msg != null)
                        output.writeString(2, msg, true);
                }
            }
            
            List<QueryExceptionType> exceptions = message.getExceptions();
            if (exceptions != null) {
                for (QueryExceptionType exception : exceptions) {
                    if (exception != null)
                        output.writeObject(3, exception, QueryExceptionType.getSchema(), true);
                }
            }
            
            if (message.result != null) {
                for (QueryMetric result : message.result) {
                    if (result != null)
                        output.writeObject(4, result, QueryMetric.getSchema(), true);
                }
            }
            
            output.writeInt32(5, message.numResults, false);
        }
        
        public void mergeFrom(Input input, QueryMetricListResponse message) throws IOException {
            LinkedList<QueryExceptionType> exceptions = null;
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setOperationTimeMS(input.readUInt64());
                        break;
                    case 2:
                        message.addMessage(input.readString());
                        break;
                    case 3:
                        if (exceptions == null)
                            exceptions = new LinkedList<QueryExceptionType>();
                        exceptions.add(input.mergeObject(null, QueryExceptionType.getSchema()));
                        break;
                    case 4:
                        if (message.result == null)
                            message.result = new ArrayList<QueryMetric>();
                        message.result.add(input.mergeObject(null, QueryMetric.getSchema()));
                        break;
                    
                    case 5:
                        message.numResults = input.readInt32();
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
                    return "operationTimeMs";
                case 2:
                    return "messages";
                case 3:
                    return "exceptions";
                case 4:
                    return "result";
                case 5:
                    return "numResults";
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
            fieldMap.put("operationTimeMs", 1);
            fieldMap.put("messages", 2);
            fieldMap.put("exceptions", 3);
            fieldMap.put("result", 4);
            fieldMap.put("numResults", 5);
        }
    };
}
