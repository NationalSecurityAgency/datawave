package datawave.webservice.result.bulk;

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
import datawave.webservice.result.BaseQueryResponse;

@XmlRootElement(name = "BulkResultsInfoResponseList")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class BulkResultsInfoResponseList extends BaseQueryResponse implements Serializable, Message<BulkResultsInfoResponseList> {

    private static final long serialVersionUID = 1L;

    @XmlElementWrapper(name = "BulkResults")
    @XmlElement(name = "BulkResult")
    List<BulkResultsInfoResponse> bulkResults = new ArrayList<BulkResultsInfoResponse>();

    public List<BulkResultsInfoResponse> getBulkResults() {
        return bulkResults;
    }

    public void setBulkResults(List<BulkResultsInfoResponse> bulkResults) {
        this.bulkResults = bulkResults;
    }

    @Override
    public Schema<BulkResultsInfoResponseList> cachedSchema() {
        return SCHEMA;
    }

    public static Schema<BulkResultsInfoResponseList> getSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<BulkResultsInfoResponseList> SCHEMA = new Schema<BulkResultsInfoResponseList>() {
        // schema methods

        public BulkResultsInfoResponseList newMessage() {
            return new BulkResultsInfoResponseList();
        }

        public Class<BulkResultsInfoResponseList> typeClass() {
            return BulkResultsInfoResponseList.class;
        }

        public String messageName() {
            return BulkResultsInfoResponseList.class.getSimpleName();
        }

        public String messageFullName() {
            return BulkResultsInfoResponseList.class.getName();
        }

        public boolean isInitialized(BulkResultsInfoResponseList message) {
            return true;
        }

        public void writeTo(Output output, BulkResultsInfoResponseList message) throws IOException {

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

            if (message.bulkResults != null) {
                for (BulkResultsInfoResponse result : message.bulkResults) {
                    if (result != null)
                        output.writeObject(6, result, BulkResultsInfoResponse.getSchema(), true);
                }
            }
        }

        public void mergeFrom(Input input, BulkResultsInfoResponseList message) throws IOException {
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
                        if (message.bulkResults == null)
                            message.bulkResults = new ArrayList<BulkResultsInfoResponse>();
                        message.bulkResults.add(input.mergeObject(null, BulkResultsInfoResponse.getSchema()));
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
                    return "bulkResults";
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
            fieldMap.put("bulkResults", 6);
        }
    };

}
