package datawave.webservice.result;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import datawave.webservice.query.exception.QueryExceptionType;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "VoidResponse")
public class VoidResponse extends BaseResponse implements Message<VoidResponse> {
    
    private static final long serialVersionUID = 1L;
    
    public static Schema<VoidResponse> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<VoidResponse> cachedSchema() {
        return SCHEMA;
    }
    
    @Override
    public String toString() {
        return "VoidResponse{} " + super.toString();
    }
    
    @XmlTransient
    private static final Schema<VoidResponse> SCHEMA = new Schema<VoidResponse>() {
        // schema methods
        
        public VoidResponse newMessage() {
            return new VoidResponse();
        }
        
        public Class<VoidResponse> typeClass() {
            return VoidResponse.class;
        }
        
        public String messageName() {
            return VoidResponse.class.getSimpleName();
        }
        
        public String messageFullName() {
            return VoidResponse.class.getName();
        }
        
        public boolean isInitialized(VoidResponse message) {
            return true;
        }
        
        public void writeTo(Output output, VoidResponse message) throws IOException {
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
            
            output.writeBool(4, message.getHasResults(), false);
        }
        
        public void mergeFrom(Input input, VoidResponse message) throws IOException {
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
                            exceptions = new LinkedList<>();
                        exceptions.add(input.mergeObject(null, QueryExceptionType.getSchema()));
                        break;
                    case 4:
                        message.setHasResults(input.readBool());
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
                    return "hasResults";
                default:
                    return null;
            }
        }
        
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }
        
        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<>();
        {
            fieldMap.put("operationTimeMs", 1);
            fieldMap.put("messages", 2);
            fieldMap.put("exceptions", 3);
            fieldMap.put("hasResults", 4);
        }
    };
}
