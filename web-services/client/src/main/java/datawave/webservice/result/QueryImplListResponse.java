package datawave.webservice.result;

import datawave.webservice.query.Query;
import datawave.webservice.query.exception.QueryExceptionType;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

@XmlRootElement(name = "QueryImplListResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryImplListResponse extends BaseResponse implements Message<QueryImplListResponse> {

    private static final long serialVersionUID = 1L;

    @XmlElement
    private List<Query> query = null;
    @XmlElement
    private int numResults = 0;

    public List<Query> getQuery() {
        return query;
    }

    public int getNumResults() {
        return numResults;
    }

    public void setQuery(List<Query> query) {
        this.query = query;
        this.numResults = this.query.size();
    }

    public void setNumResults(int numResults) {
        this.numResults = numResults;
    }

    @Override
    public Schema<QueryImplListResponse> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    public static final Schema<QueryImplListResponse> SCHEMA = new Schema<QueryImplListResponse>() {
        public QueryImplListResponse newMessage() {
            return new QueryImplListResponse();
        }

        public Class<QueryImplListResponse> typeClass() {
            return QueryImplListResponse.class;
        }

        public String messageName() {
            return QueryImplListResponse.class.getSimpleName();
        }

        public String messageFullName() {
            return QueryImplListResponse.class.getName();
        }

        public boolean isInitialized(QueryImplListResponse message) {
            return true;
        }

        public void writeTo(Output output, QueryImplListResponse message) throws IOException {

            Class<? extends Query> clazz = null;
            Schema<Query> schema = null;
            if (message.numResults != 0)
                output.writeUInt32(1, message.numResults, false);

            if (message.query != null) {
                for (Query query : message.query) {
                    if (query != null) {
                        if (null == clazz) {
                            clazz = (Class<? extends Query>) query.getClass();
                            output.writeString(6, clazz.getName(), false);
                        }
                        if ((null == schema) && (query instanceof Message)) {
                            Message<Query> m = (Message<Query>) query;
                            schema = m.cachedSchema();
                        }
                        output.writeObject(2, query, schema, true);
                    }
                }
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
        }

        public void mergeFrom(Input input, QueryImplListResponse message) throws IOException {
            LinkedList<QueryExceptionType> exceptions = null;
            int number;
            Schema<Query> schema = null;
            String queryClass = null;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.numResults = input.readUInt32();
                        break;
                    case 2:
                        if (message.query == null) {
                            message.query = new ArrayList<Query>();
                        }
                        if (null == schema) {
                            Class<? extends Query> clazz = null;
                            try {
                                clazz = (Class<? extends Query>) Class.forName(queryClass);
                            } catch (ClassNotFoundException e) {
                                throw new RuntimeException("Error finding class: " + queryClass, e);
                            }
                            try {
                                schema = ((Message) clazz.getDeclaredConstructor().newInstance()).cachedSchema();
                            } catch (Exception e) {
                                throw new RuntimeException("Error creating class: " + queryClass, e);
                            }
                        }
                        message.query.add(input.mergeObject(null, schema));
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
                        queryClass = input.readString();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }
            if (exceptions != null)
                message.setExceptions(exceptions);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "numResults";
                case 2:
                    return "query";
                case 3:
                    return "operationTimeMs";
                case 4:
                    return "messages";
                case 5:
                    return "exceptions";
                case 6:
                    return "queryClass";
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
            fieldMap.put("numResults", 1);
            fieldMap.put("query", 2);
            fieldMap.put("operationTimeMs", 3);
            fieldMap.put("messages", 4);
            fieldMap.put("exceptions", 5);
            fieldMap.put("queryClass", 6);
        }
    };

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o instanceof QueryImplListResponse) {
            QueryImplListResponse other = (QueryImplListResponse) o;
            return (this.numResults == other.numResults) && (this.query.equals(other.query));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + numResults;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("numResults: ").append(numResults);
        buf.append(", queries: ").append(query);
        return buf.toString();
    }
}
