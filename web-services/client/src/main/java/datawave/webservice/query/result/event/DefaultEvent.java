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
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.commons.lang.builder.EqualsBuilder;

import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.xml.util.StringMapAdapter;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultEvent extends EventBase<DefaultEvent,DefaultField> implements Serializable, Message<DefaultEvent>, ObjectSizeOf {

    private static final long serialVersionUID = 1L;

    @XmlElement(name = "Markings")
    @XmlJavaTypeAdapter(StringMapAdapter.class)
    private HashMap<String,String> markings = null;

    @XmlElement(name = "Metadata")
    private Metadata metadata = null;

    @XmlElementWrapper(name = "Fields")
    @XmlElement(name = "Field")
    private List<DefaultField> fields = null;

    public List<DefaultField> getFields() {
        return fields;
    }

    public void setFields(List<DefaultField> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return getMarkings() + ": " + (this.fields != null ? this.fields.toString() : "fields are null");
    }

    @Override
    public Map<String,String> getMarkings() {
        if (markings != null) {
            return markings;
        } else {
            return super.getMarkings();
        }
    }

    public void setMarkings(Map<String,String> markings) {
        if (null != markings) {
            this.markings = new HashMap<>(markings);
        } else {
            this.markings = null;
        }
        super.setMarkings(this.markings);
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DefaultEvent) {
            DefaultEvent v = (DefaultEvent) o;

            EqualsBuilder eb = new EqualsBuilder();

            eb.append(this.markings, v.markings);
            eb.append(this.metadata, v.metadata);
            eb.append(this.fields, v.fields);
            return eb.isEquals();
        }

        return false;
    }

    @Override
    public int hashCode() {
        int result = markings != null ? markings.hashCode() : 0;
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        return result;
    }

    @Override
    public Schema<DefaultEvent> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<DefaultEvent> SCHEMA = new Schema<DefaultEvent>() {
        public DefaultEvent newMessage() {
            return new DefaultEvent();
        }

        public Class<DefaultEvent> typeClass() {
            return DefaultEvent.class;
        }

        public String messageName() {
            return DefaultEvent.class.getSimpleName();
        }

        public String messageFullName() {
            return DefaultEvent.class.getName();
        }

        public boolean isInitialized(DefaultEvent message) {
            return true;
        }

        public void writeTo(Output output, DefaultEvent message) throws IOException {
            if (message.markings != null)
                output.writeObject(1, message.markings, MapSchema.SCHEMA, false);

            if (message.metadata != null) {
                output.writeObject(2, message.metadata, Metadata.getSchema(), false);
            }

            if (message.fields != null) {
                Schema<DefaultField> schema = null;
                for (DefaultField field : message.fields) {
                    if (field != null) {
                        if (schema == null) {
                            schema = field.cachedSchema();
                        }
                        output.writeObject(3, field, schema, true);
                    }
                }
            }
        }

        public void mergeFrom(Input input, DefaultEvent message) throws IOException {
            int number;
            Schema<DefaultField> schema = null;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.markings = new HashMap<String,String>();
                        input.mergeObject(message.markings, MapSchema.SCHEMA);
                        break;
                    case 2:
                        message.metadata = input.mergeObject(null, Metadata.getSchema());
                        break;
                    case 3:
                        if (message.fields == null)
                            message.fields = new ArrayList<DefaultField>();
                        if (null == schema) {
                            DefaultField field = new DefaultField();
                            schema = field.cachedSchema();
                        }
                        DefaultField f = input.mergeObject(null, schema);
                        message.fields.add(f);
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "markings";
                case 2:
                    return "metadata";
                case 3:
                    return "fields";
                default:
                    return null;
            }
        }

        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        final HashMap<String,Integer> fieldMap = new HashMap<String,Integer>();
        {
            fieldMap.put("markings", 1);
            fieldMap.put("metadata", 2);
            fieldMap.put("fields", 3);
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
