package datawave.webservice.result.keyword;

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

import datawave.webservice.query.exception.QueryExceptionType;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "DefaultTagCloudResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultTagCloudResponse extends TagCloudResponseBase implements Serializable, Message<DefaultTagCloudResponse> {

    private static final long serialVersionUID = -3616056388452103558L;

    @XmlElementWrapper(name = "TagClouds")
    @XmlElement(name = "TagCloud")
    private List<DefaultTagCloud> tagClouds = null;

    @Override
    public List<TagCloudBase> getTagClouds() {
        if (tagClouds == null) {
            return null;
        }
        List<TagCloudBase> baseEvents = new ArrayList<>(tagClouds.size());
        baseEvents.addAll(tagClouds);
        return baseEvents;
    }

    @Override
    public void setTagClouds(List<TagCloudBase> tagClouds) {
        if (tagClouds == null || tagClouds.isEmpty()) {
            this.tagClouds = null;
        } else {
            List<DefaultTagCloud> defaultTagClouds = new ArrayList<>(tagClouds.size());
            for (TagCloudBase tagCloud : tagClouds) {
                defaultTagClouds.add((DefaultTagCloud) tagCloud);
            }
            this.tagClouds = defaultTagClouds;
        }
    }

    @Override
    public void merge(TagCloudResponseBase other) {
        if (null != other.getTagClouds()) {
            if (null == this.getTagClouds()) {
                this.tagClouds = new ArrayList<>();
            }
            for (TagCloudBase tagCloud : other.getTagClouds()) {
                this.tagClouds.add((DefaultTagCloud) tagCloud);
            }
        }

        this.setOperationTimeMS(this.getOperationTimeMS() + other.getOperationTimeMS());

        // If either is partial results, then this is partial results
        if (this.isPartialResults() != other.isPartialResults()) {
            this.setPartialResults(true);
        }

        if (null != other.getMessages()) {
            if (null == this.getMessages()) {
                this.setMessages(other.getMessages());
            } else {
                this.getMessages().addAll(other.getMessages());
            }
        }

        if (null != other.getExceptions()) {
            if (null == this.getExceptions()) {
                this.setExceptions(new LinkedList<>(other.getExceptions()));
            } else {
                this.getExceptions().addAll(other.getExceptions());
            }
        }
    }

    @Override
    public Schema<DefaultTagCloudResponse> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<DefaultTagCloudResponse> SCHEMA = new Schema<>() {
        @Override
        public DefaultTagCloudResponse newMessage() {
            return new DefaultTagCloudResponse();
        }

        @Override
        public Class<DefaultTagCloudResponse> typeClass() {
            return DefaultTagCloudResponse.class;
        }

        @Override
        public String messageName() {
            return DefaultTagCloudResponse.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return DefaultTagCloudResponse.class.getName();
        }

        @Override
        public boolean isInitialized(DefaultTagCloudResponse message) {
            return true;
        }

        @Override
        public void writeTo(Output output, DefaultTagCloudResponse message) throws IOException {

            if (message.getQueryId() != null) {
                output.writeString(1, message.getQueryId(), false);
            }

            if (message.getLogicName() != null) {
                output.writeString(2, message.getLogicName(), false);
            }

            output.writeUInt64(3, message.getOperationTimeMS(), false);

            if (message.tagClouds != null) {
                Schema<DefaultTagCloud> schema = null;
                for (DefaultTagCloud tagCloud : message.tagClouds) {
                    if (schema == null) {
                        schema = tagCloud.cachedSchema();
                    }
                    output.writeObject(4, tagCloud, schema, true);
                }
            }

            List<String> messages = message.getMessages();
            if (messages != null) {
                for (String msg : messages) {
                    if (msg != null)
                        output.writeString(5, msg, true);
                }
            }

            List<QueryExceptionType> exceptions = message.getExceptions();
            if (exceptions != null) {
                for (QueryExceptionType exception : exceptions) {
                    if (exception != null)
                        output.writeObject(6, exception, QueryExceptionType.getSchema(), true);
                }
            }
        }

        @Override
        public void mergeFrom(Input input, DefaultTagCloudResponse message) throws IOException {
            LinkedList<QueryExceptionType> exceptions = null;
            int number;
            Schema<DefaultTagCloud> schema = null;
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
                        if (message.tagClouds == null)
                            message.tagClouds = new ArrayList<>();
                        if (schema == null) {
                            DefaultTagCloud cloud = new DefaultTagCloud();
                            schema = cloud.cachedSchema();
                        }
                        message.tagClouds.add(input.mergeObject(null, schema));
                        break;
                    case 5:
                        message.addMessage(input.readString());
                        break;
                    case 6:
                        if (exceptions == null)
                            exceptions = new LinkedList<>();
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

        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "queryId";
                case 2:
                    return "logicName";
                case 3:
                    return "operationTimeMs";
                case 4:
                    return "tagClouds";
                case 5:
                    return "messages";
                case 6:
                    return "exceptions";
                default:
                    return null;
            }
        }

        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }

        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<>();
        {
            fieldMap.put("queryId", 1);
            fieldMap.put("logicName", 2);
            fieldMap.put("operationTimeMs", 3);
            fieldMap.put("tagClouds", 4);
            fieldMap.put("messages", 5);
            fieldMap.put("exceptions", 6);
        }
    };
}
