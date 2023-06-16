package datawave.webservice.results.mr;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "MapReduceInfoResponseList")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class MapReduceInfoResponseList extends BaseResponse implements Serializable, Message<MapReduceInfoResponseList> {
    private static final long serialVersionUID = 1L;

    @XmlElementWrapper(name = "MapReduceInfoResponseList")
    @XmlElement(name = "MapReduceInfoResponse")
    List<MapReduceInfoResponse> results = new ArrayList<MapReduceInfoResponse>();

    public List<MapReduceInfoResponse> getResults() {
        return results;
    }

    public void setResults(List<MapReduceInfoResponse> results) {
        this.results = results;
    }

    @Override
    public Schema<MapReduceInfoResponseList> cachedSchema() {
        return SCHEMA;
    }

    public static Schema<MapReduceInfoResponseList> getSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<MapReduceInfoResponseList> SCHEMA = new Schema<MapReduceInfoResponseList>() {
        // schema methods

        public MapReduceInfoResponseList newMessage() {
            return new MapReduceInfoResponseList();
        }

        public Class<MapReduceInfoResponseList> typeClass() {
            return MapReduceInfoResponseList.class;
        }

        public String messageName() {
            return MapReduceInfoResponseList.class.getSimpleName();
        }

        public String messageFullName() {
            return MapReduceInfoResponseList.class.getName();
        }

        public boolean isInitialized(MapReduceInfoResponseList message) {
            return true;
        }

        public void writeTo(Output output, MapReduceInfoResponseList message) throws IOException {

            if (message.getResults() != null) {
                for (MapReduceInfoResponse response : message.getResults()) {
                    if (null != response) {
                        output.writeObject(1, response, MapReduceInfoResponse.getSchema(), true);
                    }
                }
            }

            output.writeUInt64(2, message.getOperationTimeMS(), false);

            List<String> messages = message.getMessages();
            if (messages != null) {
                for (String msg : messages) {
                    if (msg != null)
                        output.writeString(3, msg, true);
                }
            }

            List<QueryExceptionType> exceptions = message.getExceptions();
            if (exceptions != null) {
                for (QueryExceptionType exception : exceptions) {
                    if (exception != null)
                        output.writeObject(4, exception, QueryExceptionType.getSchema(), true);
                }
            }

        }

        public void mergeFrom(Input input, MapReduceInfoResponseList message) throws IOException {
            List<MapReduceInfoResponse> responses = null;
            LinkedList<QueryExceptionType> exceptions = null;
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        if (responses == null)
                            responses = new ArrayList<MapReduceInfoResponse>();
                        responses.add(input.mergeObject(null, MapReduceInfoResponse.getSchema()));
                        break;
                    case 2:
                        message.setOperationTimeMS(input.readUInt64());
                        break;
                    case 3:
                        message.addMessage(input.readString());
                        break;
                    case 4:
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
            if (responses != null)
                message.setResults(responses);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "results";
                case 2:
                    return "operationTimeMs";
                case 3:
                    return "messages";
                case 4:
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
            fieldMap.put("results", 1);
            fieldMap.put("operationTimeMs", 2);
            fieldMap.put("messages", 3);
            fieldMap.put("exceptions", 4);
        }
    };

}
