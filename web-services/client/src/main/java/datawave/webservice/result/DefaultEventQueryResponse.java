package datawave.webservice.result;

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
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.EventBase;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "DefaultEventQueryResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultEventQueryResponse extends EventQueryResponseBase implements TotalResultsAware, Serializable, Message<DefaultEventQueryResponse> {

    private static final long serialVersionUID = 1L;

    @XmlElement(name = "TotalEvents")
    private Long totalEvents = null;

    @XmlElement(name = "ReturnedEvents")
    private Long returnedEvents = null;

    /**
     * A list of unique field names contained in this response's collection of Events specific to only this response.
     *
     * This list of fields is only determined by this object, and not the larger complete query result set. For a given query, is it likely that the contents of
     * the list will change.
     */
    @XmlElementWrapper(name = "Fields")
    @XmlElement(name = "Field")
    private List<String> fields = null;

    @XmlElementWrapper(name = "Events")
    @XmlElement(name = "Event")
    private List<DefaultEvent> events = null;

    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.result.EventQueryResponse#getTotalEvents()
     */
    @Override
    public Long getTotalEvents() {
        return totalEvents;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.result.EventQueryResponse#getReturnedEvents()
     */
    @Override
    public Long getReturnedEvents() {
        return returnedEvents;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.result.EventQueryResponse#getFields()
     */
    @Override
    public List<String> getFields() {
        return fields;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.result.EventQueryResponse#getEvents()
     */
    @Override
    public List<EventBase> getEvents() {
        if (events == null) {
            return null;
        }
        List<EventBase> baseEvents = new ArrayList<EventBase>(events.size());
        baseEvents.addAll(events);
        return baseEvents;
    }

    public void setTotalEvents(Long totalEvents) {
        this.totalEvents = totalEvents;
    }

    public void setReturnedEvents(Long returnedEvents) {
        this.returnedEvents = returnedEvents;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.result.EventQueryResponse#setFields(java.util.List)
     */
    @Override
    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.result.EventQueryResponse#setEvents(java.util.List)
     */
    @Override
    public void setEvents(List<EventBase> entries) {
        if (entries == null || entries.isEmpty()) {
            this.events = null;
        } else {
            List<DefaultEvent> events = new ArrayList<>(entries.size());
            for (EventBase event : entries) {
                events.add((DefaultEvent) event);
            }
            this.events = events;
        }
    }

    public static Schema<DefaultEventQueryResponse> getSchema() {
        return SCHEMA;
    }

    @Override
    public Schema<DefaultEventQueryResponse> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<DefaultEventQueryResponse> SCHEMA = new Schema<DefaultEventQueryResponse>() {
        // schema methods

        @Override
        public DefaultEventQueryResponse newMessage() {
            return new DefaultEventQueryResponse();
        }

        @Override
        public Class<DefaultEventQueryResponse> typeClass() {
            return DefaultEventQueryResponse.class;
        }

        @Override
        public String messageName() {
            return DefaultEventQueryResponse.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return DefaultEventQueryResponse.class.getName();
        }

        @Override
        public boolean isInitialized(DefaultEventQueryResponse message) {
            return true;
        }

        @Override
        public void writeTo(Output output, DefaultEventQueryResponse message) throws IOException {

            if (message.totalEvents != null) {
                output.writeUInt64(1, message.totalEvents, false);
            }

            if (message.returnedEvents != null) {
                output.writeUInt64(2, message.returnedEvents, false);
            }

            if (message.fields != null) {
                for (String msg : message.fields) {
                    if (msg != null)
                        output.writeString(3, msg, true);
                }
            }

            if (message.events != null) {
                Schema<DefaultEvent> schema = null;
                for (DefaultEvent event : message.events) {
                    if (event != null) {
                        if (schema == null) {
                            schema = event.cachedSchema();
                        }
                        output.writeObject(4, event, event.cachedSchema(), true);
                    }
                }
            }

            if (message.getQueryId() != null) {
                output.writeString(5, message.getQueryId(), false);
            }

            if (message.getLogicName() != null) {
                output.writeString(6, message.getLogicName(), false);
            }

            output.writeUInt64(7, message.getOperationTimeMS(), false);

            List<String> messages = message.getMessages();
            if (messages != null) {
                for (String msg : messages) {
                    if (msg != null)
                        output.writeString(8, msg, true);
                }
            }

            List<QueryExceptionType> exceptions = message.getExceptions();
            if (exceptions != null) {
                for (QueryExceptionType exception : exceptions) {
                    if (exception != null)
                        output.writeObject(9, exception, QueryExceptionType.getSchema(), true);
                }
            }
        }

        @Override
        public void mergeFrom(Input input, DefaultEventQueryResponse message) throws IOException {
            LinkedList<QueryExceptionType> exceptions = null;
            int number;
            Schema<DefaultEvent> schema = null;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.totalEvents = input.readUInt64();
                        break;
                    case 2:
                        message.returnedEvents = input.readUInt64();
                        break;
                    case 3:
                        if (message.fields == null)
                            message.fields = new ArrayList<String>();
                        message.fields.add(input.readString());
                        break;
                    case 4:
                        if (message.events == null)
                            message.events = new ArrayList<DefaultEvent>();
                        if (null == schema) {
                            DefaultEvent event = new DefaultEvent();
                            schema = event.cachedSchema();
                        }
                        message.events.add(input.mergeObject(null, schema));
                        break;
                    case 5:
                        message.setQueryId(input.readString());
                        break;
                    case 6:
                        message.setLogicName(input.readString());
                        break;
                    case 7:
                        message.setOperationTimeMS(input.readUInt64());
                        break;
                    case 8:
                        message.addMessage(input.readString());
                        break;
                    case 9:
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

        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "totalEvents";
                case 2:
                    return "returnedEvents";
                case 3:
                    return "fields";
                case 4:
                    return "events";
                case 5:
                    return "queryId";
                case 6:
                    return "logicName";
                case 7:
                    return "operationTimeMs";
                case 8:
                    return "messages";
                case 9:
                    return "exceptions";
                default:
                    return null;
            }
        }

        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<String,Integer>();
        {
            fieldMap.put("totalEvents", 1);
            fieldMap.put("returnedEvents", 2);
            fieldMap.put("fields", 3);
            fieldMap.put("events", 4);
            fieldMap.put("queryId", 5);
            fieldMap.put("logicName", 6);
            fieldMap.put("operationTimeMs", 7);
            fieldMap.put("messages", 8);
            fieldMap.put("exceptions", 9);
        }
    };

    @Override
    public void setTotalResults(long totalResults) {
        this.totalEvents = totalResults;
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.result.EventQueryResponse#getTotalResults()
     */
    @Override
    public long getTotalResults() {
        return totalEvents == null ? -1 : totalEvents;
    }

    @Override
    public void merge(EventQueryResponseBase other) {
        if (null != other.getEvents()) {
            if (null == this.events) {
                this.events = new ArrayList<DefaultEvent>();
            }
            for (EventBase event : other.getEvents()) {
                this.events.add((DefaultEvent) event);
            }
        }

        if (null != other.getFields()) {
            if (null == this.fields) {
                this.fields = new ArrayList<String>(other.getFields());
            } else {
                this.fields.addAll(other.getFields());
            }
        }

        this.returnedEvents += other.getReturnedEvents();
        this.totalEvents += other.getTotalEvents();
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
                this.setExceptions(new LinkedList<QueryExceptionType>(other.getExceptions()));
            } else {
                this.getExceptions().addAll(other.getExceptions());
            }
        }
    }

};
