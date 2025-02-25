package datawave.webservice.query.exception;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryExceptionType implements Serializable, Message<QueryExceptionType> {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "Message")
    private String message;
    
    @XmlElement(name = "Cause")
    private String cause;
    
    @XmlElement(name = "Code")
    private String code;
    
    public QueryExceptionType() {
        super();
    }
    
    public QueryExceptionType(String message, String cause, String code) {
        super();
        this.message = message;
        this.cause = cause;
        this.code = code;
    }
    
    public String getMessage() {
        return message;
    }
    
    public String getCause() {
        return cause;
    }
    
    public String getCode() {
        return code;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public void setCause(String cause) {
        this.cause = cause;
    }
    
    public void setCode(String code) {
        this.code = code;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        QueryExceptionType that = (QueryExceptionType) o;
        return Objects.equals(message, that.message) && Objects.equals(cause, that.cause) && Objects.equals(code, that.code);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(message, cause, code);
    }
    
    @Override
    public String toString() {
        return "QueryExceptionType{" + "message='" + message + '\'' + ", cause='" + cause + '\'' + ", code='" + code + '\'' + '}';
    }
    
    public static Schema<QueryExceptionType> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<QueryExceptionType> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<QueryExceptionType> SCHEMA = new Schema<QueryExceptionType>() {
        
        @Override
        public QueryExceptionType newMessage() {
            return new QueryExceptionType();
        }
        
        @Override
        public Class<? super QueryExceptionType> typeClass() {
            return QueryExceptionType.class;
        }
        
        @Override
        public String messageName() {
            return QueryExceptionType.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return QueryExceptionType.class.getName();
        }
        
        @Override
        public boolean isInitialized(QueryExceptionType message) {
            return true;
        }
        
        @Override
        public void writeTo(Output output, QueryExceptionType message) throws IOException {
            if (message.message != null)
                output.writeString(1, message.message, false);
            if (message.cause != null)
                output.writeString(2, message.cause, false);
            if (message.code != null)
                output.writeString(3, message.code, false);
        }
        
        @Override
        public void mergeFrom(Input input, QueryExceptionType message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.message = input.readString();
                        break;
                    case 2:
                        message.cause = input.readString();
                        break;
                    case 3:
                        message.code = input.readString();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }
        
        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "message";
                case 2:
                    return "cause";
                case 3:
                    return "code";
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }
        
        private final HashMap<String,Integer> fieldMap = new HashMap<String,Integer>();
        {
            fieldMap.put("message", 1);
            fieldMap.put("cause", 2);
            fieldMap.put("code", 3);
        }
    };
}
