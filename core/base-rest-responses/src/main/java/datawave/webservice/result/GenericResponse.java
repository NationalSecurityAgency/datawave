package datawave.webservice.result;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import datawave.webservice.query.exception.QueryExceptionType;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import io.protostuff.UninitializedMessageException;

@XmlRootElement(name = "GenericResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class GenericResponse<T> extends BaseResponse implements Message<GenericResponse<T>> {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "Result")
    private T result = null;
    
    public T getResult() {
        return result;
    }
    
    public void setResult(T result) {
        this.result = result;
    }
    
    @Override
    public Schema<GenericResponse<T>> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private Schema<GenericResponse<T>> SCHEMA = new GenericResponseSchema();
    
    public class GenericResponseSchema implements Schema<GenericResponse<T>> {
        
        public GenericResponse<T> newMessage() {
            return new GenericResponse<>();
        }
        
        @SuppressWarnings("rawtypes")
        public Class<GenericResponse> typeClass() {
            return GenericResponse.class;
        }
        
        public String messageName() {
            return GenericResponse.class.getSimpleName();
        }
        
        public String messageFullName() {
            return GenericResponse.class.getName();
        }
        
        public boolean isInitialized(GenericResponse<T> message) {
            return result != null;
        }
        
        public void writeTo(Output output, GenericResponse<T> message) throws IOException {
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
                output.writeString(4, message.result.getClass().getName(), false);
                if (message.result instanceof String)
                    output.writeString(5, (String) message.result, false);
                else if (message.result instanceof Integer)
                    output.writeSInt32(6, (Integer) message.result, false);
                else if (message.result instanceof Long)
                    output.writeSInt64(7, (Long) message.result, false);
                else if (message.result instanceof Boolean)
                    output.writeBool(8, (Boolean) message.result, false);
                else if (message.result instanceof Float)
                    output.writeFloat(9, (Float) message.result, false);
                else if (message.result instanceof Double)
                    output.writeDouble(10, (Double) message.result, false);
                else if (message.result instanceof byte[])
                    output.writeByteArray(11, (byte[]) message.result, false);
            } else {
                throw new UninitializedMessageException(message);
            }
        }
        
        @SuppressWarnings("unchecked")
        public void mergeFrom(Input input, GenericResponse<T> message) throws IOException {
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
                        // ignore
                        break;
                    case 5:
                        message.result = (T) input.readString();
                        break;
                    case 6:
                        message.result = (T) Boolean.valueOf(input.readBool());
                        break;
                    case 7:
                        message.result = (T) Integer.valueOf(input.readSInt32());
                        break;
                    case 8:
                        message.result = (T) Long.valueOf(input.readSInt64());
                        break;
                    case 9:
                        message.result = (T) Float.valueOf(input.readFloat());
                        break;
                    case 10:
                        message.result = (T) Double.valueOf(input.readDouble());
                        break;
                    case 11:
                        message.result = (T) input.readByteArray();
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
                    return "resultClassName";
                case 5:
                    return "resultAsString";
                case 6:
                    return "resultAsBoolean";
                case 7:
                    return "resultAsInt";
                case 8:
                    return "resultAsLong";
                case 9:
                    return "resultAsFloat";
                case 10:
                    return "resultAsDouble";
                case 11:
                    return "resultAsBytes";
                case 12:
                    return "resultAsDescription";
                default:
                    return null;
            }
        }
        
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }
        
        final HashMap<String,Integer> fieldMap = new HashMap<>();
        {
            fieldMap.put("operationTimeMs", 1);
            fieldMap.put("messages", 2);
            fieldMap.put("exceptions", 3);
            fieldMap.put("resultClassName", 4);
            fieldMap.put("resultAsString", 5);
            fieldMap.put("resultAsBoolean", 6);
            fieldMap.put("resultAsInt", 7);
            fieldMap.put("resultAsLong", 8);
            fieldMap.put("resultAsFloat", 9);
            fieldMap.put("resultAsDouble", 10);
            fieldMap.put("resultAsBytes", 11);
            fieldMap.put("resultAsDescription", 12);
        }
    }
}
