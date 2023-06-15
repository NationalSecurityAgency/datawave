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
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import datawave.data.type.Type;
import datawave.webservice.query.util.TypedValue;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.collect.Maps;

/**
 * This object is contained inside of Event objects to describe name/value pairs of data in the Event.
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class SimpleField extends FieldBase<SimpleField> implements Serializable, Message<SimpleField> {

    private static final long serialVersionUID = 1L;

    @XmlAttribute(name = "columnVisibility")
    private String columnVisibility;
    @XmlElementWrapper(name = "Markings")
    private Map<String,String> markings;
    @XmlAttribute(name = "timestamp")
    private Long timestamp;
    @XmlAttribute(name = "name")
    private String name;
    @XmlElement(name = "Value")
    private TypedValue value;

    public SimpleField() {}

    public SimpleField(String name, Map<String,String> markings, String columnVisibility, Long timestamp, Object value) {
        super();
        this.name = name;
        this.markings = markings;
        this.columnVisibility = columnVisibility;
        this.timestamp = timestamp;
        this.value = new TypedValue(value);
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getValueString() {
        if (value.getValue() instanceof Type<?>) {
            return ((Type<?>) value.getValue()).getDelegate().toString();
        } else if (value.getValue() instanceof String) {
            return (String) value.getValue();
        } else {
            return value.getValue().toString();
        }
    }

    public TypedValue getTypedValue() {
        return this.value;
    }

    public Object getValueOfTypedValue() {
        return (null == value) ? null : value.getValue();
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(Object value) {
        this.value = new TypedValue(value);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("Field [");
        buf.append("markings=").append(markings);
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
        return new HashCodeBuilder(17, 37).append(columnVisibility).append(markings).append(timestamp).append(name).append(value).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SimpleField) {
            SimpleField v = (SimpleField) o;

            EqualsBuilder eb = new EqualsBuilder();

            eb.append(this.name, v.name);
            eb.append(this.timestamp, v.timestamp);
            eb.append(this.columnVisibility, v.columnVisibility);
            eb.append(this.markings, v.markings);
            eb.append(this.value, v.value);
            return eb.isEquals();
        }

        return false;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private void assureMarkings() {
        if (this.markings == null)
            this.markings = Maps.newHashMap();
    }

    public Map<String,String> getMarkings() {
        this.assureMarkings();
        return markings;
    }

    public void setMarkings(Map<String,String> markings) {
        this.markings = markings;
    }

    public static Schema<SimpleField> getSchema() {
        return SCHEMA;
    }

    // @Override
    public Schema<SimpleField> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<Map<String,String>> MAP_SCHEMA = new MapSchema();

    @XmlTransient
    private static final Schema<SimpleField> SCHEMA = new Schema<SimpleField>() {

        @Override
        public SimpleField newMessage() {
            return new SimpleField();
        }

        @Override
        public Class<? super SimpleField> typeClass() {
            return SimpleField.class;
        }

        @Override
        public String messageName() {
            return SimpleField.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return SimpleField.class.getName();
        }

        @Override
        public boolean isInitialized(SimpleField message) {
            return true;
        }

        @Override
        public void writeTo(Output output, SimpleField message) throws IOException {
            if (message.columnVisibility != null)
                output.writeString(1, message.columnVisibility, false);
            output.writeUInt64(2, message.timestamp, false);
            if (message.name != null)
                output.writeString(3, message.name, false);
            if (message.value != null)
                output.writeObject(4, message.value, message.value.cachedSchema(), false);
            if (message.markings != null) {
                output.writeObject(5, message.markings, MAP_SCHEMA, false);
            }
        }

        @Override
        public void mergeFrom(Input input, SimpleField message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.columnVisibility = input.readString();
                        break;
                    case 2:
                        message.timestamp = input.readUInt64();
                        break;
                    case 3:
                        message.name = input.readString();
                        break;
                    case 4:
                        message.value = input.mergeObject(null, TypedValue.getSchema());
                        break;
                    case 5:
                        if (message.markings == null)
                            message.markings = Maps.newHashMap();
                        Map<String,String> markings = input.mergeObject(null, MAP_SCHEMA);
                        message.markings.putAll(markings);
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
                    return "columnVisibility";
                case 2:
                    return "timestamp";
                case 3:
                    return "name";
                case 4:
                    return "value";
                case 5:
                    return "markings";
                default:
                    return null;
            }
        }

        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        private final HashMap<String,Integer> fieldMap = new HashMap<>();
        {
            fieldMap.put("columnVisibility", 1);
            fieldMap.put("timestamp", 2);
            fieldMap.put("name", 3);
            fieldMap.put("value", 4);
            fieldMap.put("markings", 5);
        }
    };

    public String getColumnVisibility() {
        return columnVisibility;
    }

    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
}
