package nsa.datawave.webservice.result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import nsa.datawave.webservice.HtmlProvider;
import nsa.datawave.webservice.query.exception.QueryExceptionType;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "VoidResponse")
public class VoidResponse extends BaseResponse implements Message<VoidResponse>, HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private List<String> messsages = new ArrayList<String>();
    
    public VoidResponse() {
        super();
    }
    
    public static Schema<VoidResponse> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<VoidResponse> cachedSchema() {
        return SCHEMA;
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
                            exceptions = new LinkedList<QueryExceptionType>();
                        exceptions.add(input.mergeObject(null, QueryExceptionType.getSchema()));
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
        }
    };
    
    @Override
    public String getTitle() {
        return "Void Response";
    }
    
    @Override
    public String getHeadContent() {
        return "";
    }
    
    @Override
    public String getPageHeader() {
        return VoidResponse.class.getName();
    }
    
    public static final String BR = "<br/>";
    
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        builder.append("<b>MESSAGES:</b>").append(BR);
        List<String> messages = this.getMessages();
        if (messages != null) {
            for (String msg : messages) {
                if (msg != null)
                    builder.append(msg).append(BR);
            }
        }
        
        builder.append("<b>EXCEPTIONS:</b>").append(BR);
        List<QueryExceptionType> exceptions = this.getExceptions();
        if (exceptions != null) {
            for (QueryExceptionType exception : exceptions) {
                if (exception != null)
                    builder.append(exception).append(", ").append(QueryExceptionType.getSchema()).append(BR);
            }
        }
        return builder.toString();
    }
}
