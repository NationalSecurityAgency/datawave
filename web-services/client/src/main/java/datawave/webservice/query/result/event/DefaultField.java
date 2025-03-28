package datawave.webservice.query.result.event;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;

import datawave.data.type.Type;
import datawave.webservice.query.util.TypedValue;
import datawave.webservice.xml.util.StringMapAdapter;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

/**
 * This object is contained inside of Event objects to describe name/value pairs of data in the Event. Even though the columnVisibility of the Field has already
 * been interpreted, it is sometimes necessary to have the original columnVisibility (e.g. for the MutableMedata service in the Modification operations) For
 * this reason, the columnVisibility attribute has been added to the Field object to indicate how that data was marked originally in Accumulo.
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultField extends FieldBase<DefaultField> implements Serializable, Message<DefaultField> {

    private static final long serialVersionUID = 1L;

    @XmlElement(name = "Markings")
    @XmlJavaTypeAdapter(StringMapAdapter.class)
    private HashMap<String,String> markings;
    @XmlAttribute(name = "columnVisibility")
    private String columnVisibility;
    @XmlAttribute(name = "timestamp")
    private Long timestamp;
    @XmlAttribute(name = "name")
    private String name;
    @XmlElement(name = "Value")
    private TypedValue value;

    public DefaultField() {}

    public DefaultField(String name, String columnVisibility, Map<String,String> markings, Long timestamp, Object value) {
        super();
        this.name = name;
        this.markings = new HashMap<String,String>(markings);
        this.columnVisibility = columnVisibility;
        this.timestamp = timestamp;
        this.value = new TypedValue(value);
    }

    public DefaultField(String name, String columnVisibility, Long timestamp, Object value) {
        super();
        this.name = name;
        this.columnVisibility = columnVisibility;
        this.timestamp = timestamp;
        this.value = new TypedValue(value);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.webservice.query.result.event.FieldInterface#setMarkings(java.util.Map)
     */
    @Override
    public void setMarkings(Map<String,String> markings) {
        this.markings = new HashMap<String,String>(markings);
    }

    public Map<String,String> getMarkings() {
        return markings;
    }

    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public String getColumnVisibility() {
        return columnVisibility;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setTypedValue(TypedValue value) {
        this.value = value;
    }

    public TypedValue getTypedValue() {
        return this.value;
    }

    @JsonIgnore
    public void setValue(Object value) {
        if (value instanceof TypedValue) {
            this.value = (TypedValue) value;
        } else {
            this.value = new TypedValue(value);
        }
    }

    @JsonIgnore
    @XmlTransient
    public Object getValueOfTypedValue() {
        return (null == value) ? null : value.getValue();
    }

    @JsonIgnore
    @XmlTransient
    public String getValueString() {
        if (value.getValue() instanceof Type<?>) {
            return ((Type<?>) value.getValue()).getDelegate().toString();
        } else if (value.getValue() instanceof String) {
            return (String) value.getValue();
        } else {
            return value.getValue().toString();
        }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Field [markings=").append(markings);
        buf.append(" columnVisibility=").append(columnVisibility);
        buf.append(" timestamp=").append(timestamp);
        buf.append(" name=").append(name);
        buf.append(" value= ");
        if (null != value)
            buf.append(value).append("] ");
        else
            buf.append("null ]");
        return buf.toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(markings).append(columnVisibility).append(timestamp).append(name).append(value).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DefaultField) {
            DefaultField v = (DefaultField) o;

            EqualsBuilder eb = new EqualsBuilder();

            eb.append(this.name, v.name);
            eb.append(this.timestamp, v.timestamp);
            eb.append(this.markings, v.markings);
            eb.append(this.columnVisibility, v.columnVisibility);
            eb.append(this.value, v.value);
            return eb.isEquals();
        }

        return false;
    }

    @Override
    public Schema<DefaultField> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<DefaultField> SCHEMA = new Schema<DefaultField>() {

        @Override
        public DefaultField newMessage() {
            return new DefaultField();
        }

        @Override
        public Class<? super DefaultField> typeClass() {
            return DefaultField.class;
        }

        @Override
        public String messageName() {
            return DefaultField.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return DefaultField.class.getName();
        }

        @Override
        public boolean isInitialized(DefaultField message) {
            return true;
        }

        @Override
        public void writeTo(Output output, DefaultField message) throws IOException {
            if (message.markings != null)
                output.writeObject(1, message.markings, MapSchema.SCHEMA, false);
            if (message.columnVisibility != null)
                output.writeString(2, message.columnVisibility, false);
            output.writeUInt64(3, message.timestamp, false);
            if (message.name != null)
                output.writeString(4, message.name, false);
            if (message.value != null)
                output.writeObject(5, message.value, message.value.cachedSchema(), false);
        }

        @Override
        public void mergeFrom(Input input, DefaultField message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.markings = new HashMap<String,String>();
                        input.mergeObject(message.markings, MapSchema.SCHEMA);
                        break;
                    case 2:
                        message.columnVisibility = input.readString();
                        break;
                    case 3:
                        message.timestamp = input.readUInt64();
                        break;
                    case 4:
                        message.name = input.readString();
                        break;
                    case 5:
                        message.value = input.mergeObject(null, TypedValue.getSchema());
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
                    return "markings";
                case 2:
                    return "columnVisibility";
                case 3:
                    return "timestamp";
                case 4:
                    return "name";
                case 5:
                    return "value";
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
            fieldMap.put("markings", 1);
            fieldMap.put("columnVisibility", 2);
            fieldMap.put("timestamp", 3);
            fieldMap.put("name", 4);
            fieldMap.put("value", 5);
        }
    };
}
