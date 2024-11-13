package datawave.webservice.query.result.event;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;

import com.google.common.collect.Maps;

import datawave.webservice.query.data.ObjectSizeOf;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class SimpleEvent extends EventBase<SimpleEvent,SimpleField> implements Serializable, Message<SimpleEvent>, ObjectSizeOf {

    private static final long serialVersionUID = 1L;

    @XmlElementWrapper(name = "Markings")
    private Map<String,String> markings;

    @XmlElement(name = "Metadata")
    private Metadata metadata = null;

    @XmlElementWrapper(name = "Fields")
    @XmlElement(name = "Field")
    private List<SimpleField> fields = null;

    public List<SimpleField> getFields() {
        return fields;
    }

    public void setFields(List<SimpleField> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return getMarkings() + ": " + this.fields;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public static Schema<SimpleEvent> getSchema() {
        return SCHEMA;
    }

    // @Override
    public Schema<SimpleEvent> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<SimpleEvent> SCHEMA = new Schema<SimpleEvent>() {
        public SimpleEvent newMessage() {
            return new SimpleEvent();
        }

        public Class<SimpleEvent> typeClass() {
            return SimpleEvent.class;
        }

        public String messageName() {
            return SimpleEvent.class.getSimpleName();
        }

        public String messageFullName() {
            return SimpleEvent.class.getName();
        }

        public boolean isInitialized(SimpleEvent message) {
            return true;
        }

        public void writeTo(Output output, SimpleEvent message) throws IOException {
            if (message.metadata != null) {
                output.writeObject(1, message.metadata, Metadata.getSchema(), false);
            }

            if (message.fields != null) {
                for (SimpleField field : message.fields) {
                    if (field != null)
                        output.writeObject(2, field, SimpleField.getSchema(), true);
                }
            }
            if (message.markings != null) {
                output.writeObject(3, message.markings, MapSchema.SCHEMA, false);
            }
        }

        public void mergeFrom(Input input, SimpleEvent message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.metadata = input.mergeObject(null, Metadata.getSchema());
                        break;
                    case 2:
                        if (message.fields == null)
                            message.fields = new ArrayList<>();
                        SimpleField f = input.mergeObject(null, SimpleField.getSchema());
                        message.fields.add(f);
                        break;
                    case 3:
                        if (message.markings == null)
                            message.markings = Maps.newHashMap();
                        input.mergeObject(message.markings, MapSchema.SCHEMA);
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "metadata";
                case 2:
                    return "fields";
                case 3:
                    return "markings";
                case 4:
                    return "payload";
                default:
                    return null;
            }
        }

        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        final HashMap<String,Integer> fieldMap = new HashMap<>();
        {
            fieldMap.put("metadata", 1);
            fieldMap.put("fields", 2);
            fieldMap.put("markings", 3);
        }
    };

    // the approximate size in bytes of this event
    private long size = -1;

    /**
     * @param size
     *            the approximate size of this event in bytes
     */
    public void setSizeInBytes(long size) {
        this.size = size;
    }

    /**
     * @return The set size in bytes, -1 if unset
     */
    public long getSizeInBytes() {
        return this.size;
    }

    /**
     * Get the approximate size of this event in bytes. Used by the ObjectSizeOf mechanism in the webservice. Throws an exception if the local size was not set
     * to allow the ObjectSizeOf mechanism to do its thang.
     */
    @Override
    public long sizeInBytes() {
        if (size <= 0) {
            throw new UnsupportedOperationException();
        }
        return this.size;
    }

}
