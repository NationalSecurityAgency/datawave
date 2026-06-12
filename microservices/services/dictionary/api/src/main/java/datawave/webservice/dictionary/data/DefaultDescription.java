package datawave.webservice.dictionary.data;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import datawave.marking.AccessExpressionMarkings;
import datawave.marking.Markings;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultDescription extends DescriptionBase<DefaultDescription> implements Serializable, Message<DefaultDescription> {
    private static final long serialVersionUID = 6756537335803261872L;

    public DefaultDescription() {}

    public DefaultDescription(String description) {
        this.description = description;
    }

    public DefaultDescription(String description, Markings<?> markings) {
        this.description = description;
        this.markings = markings;
    }

    @XmlElement(name = "description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    @XmlElement(name = "markings", type = AccessExpressionMarkings.class)
    public Markings<?> getMarkings() {
        return this.markings;
    }

    @Override
    public void setMarkings(Markings<?> markings) {
        this.markings = markings;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DefaultDescription) {
            DefaultDescription other = (DefaultDescription) o;
            return Objects.equals(this.description, other.description);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return this.description.hashCode();
    }

    @Override
    public String toString() {
        return getDescription();
    }

    public static Schema<DefaultDescription> getSchema() {
        return SCHEMA;
    }

    @Override
    /*
     * (non-Javadoc)
     *
     * @see io.protostuff.Message#cachedSchema()
     */
    public Schema<DefaultDescription> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<DefaultDescription> SCHEMA = new Schema<>() {
        public DefaultDescription newMessage() {
            return new DefaultDescription();
        }

        public Class<DefaultDescription> typeClass() {
            return DefaultDescription.class;
        }

        public String messageName() {
            return DefaultDescription.class.getSimpleName();
        }

        public String messageFullName() {
            return DefaultDescription.class.getName();
        }

        public boolean isInitialized(DefaultDescription message) {
            return true;
        }

        public void writeTo(Output output, DefaultDescription message) throws IOException {
            if (message.description != null) {
                output.writeString(1, message.description, false);
            }
            if (message.markings != null)
                output.writeObject(2, (AccessExpressionMarkings) message.markings, AccessExpressionMarkings.SCHEMA, false);
        }

        public void mergeFrom(Input input, DefaultDescription message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.description = input.readString();
                        break;
                    case 2:
                        message.markings = AccessExpressionMarkings.builder().build();
                        input.mergeObject((AccessExpressionMarkings) message.markings, AccessExpressionMarkings.SCHEMA);
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
                    return "description";
                case 2:
                    return "markings";
                default:
                    return null;
            }
        }

        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }

        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<String,Integer>();
        {
            fieldMap.put("description", 1);
            fieldMap.put("markings", 2);
        }
    };

}
