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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

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
public class DefaultFieldCardinality extends FieldCardinalityBase implements Serializable, Message<DefaultFieldCardinality> {
    
    private static final long serialVersionUID = 1L;
    
    @XmlAttribute(name = "field")
    private String field;
    @XmlAttribute(name = "columnVisibility")
    private String columnVisibility;
    @XmlAttribute(name = "lower")
    private String lower;
    @XmlAttribute(name = "upper")
    private String upper;
    @XmlElement(name = "cardinality")
    private Long cardinality;
    
    public DefaultFieldCardinality() {}
    
    public DefaultFieldCardinality(String field, String columnVisibility, String lower, String upper, Long cardinality) {
        super();
        this.field = field;
        this.columnVisibility = columnVisibility;
        this.lower = lower;
        this.upper = upper;
        this.cardinality = cardinality;
    }
    
    @Override
    public String getField() {
        return field;
    }
    
    @Override
    public void setField(String field) {
        this.field = field;
    }
    
    @Override
    public void setMarkings(Map<String,String> markings) {
        this.markings = markings;
    }
    
    public Map<String,String> getMarkings() {
        return this.markings;
    }
    
    public Long getCardinality() {
        return cardinality;
    }
    
    public void setCardinality(Long cardinality) {
        this.cardinality = cardinality;
    }
    
    @Override
    public String toString() {
        return " field=" + field + " columnVisibility=" + columnVisibility + " cardinality=" + cardinality + " lower=" + lower + " upper= " + upper + "] ";
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(field).append(columnVisibility).append(cardinality).append(lower).append(upper).hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof DefaultFieldCardinality) {
            DefaultFieldCardinality v = (DefaultFieldCardinality) o;
            
            EqualsBuilder eb = new EqualsBuilder();
            
            eb.append(this.field, v.field);
            eb.append(this.columnVisibility, v.columnVisibility);
            eb.append(this.lower, v.lower);
            eb.append(this.upper, v.upper);
            eb.append(this.cardinality, v.cardinality);
            return eb.isEquals();
        }
        
        return false;
    }
    
    public String getLower() {
        return lower;
    }
    
    public String getUpper() {
        return upper;
    }
    
    public void setLower(String lower) {
        this.lower = lower;
    }
    
    public void setUpper(String upper) {
        this.upper = upper;
    }
    
    public static Schema<DefaultFieldCardinality> getSchema() {
        return SCHEMA;
    }
    
    public String getColumnVisibility() {
        return columnVisibility;
    }
    
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
    
    @Override
    public Schema<DefaultFieldCardinality> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<DefaultFieldCardinality> SCHEMA = new Schema<DefaultFieldCardinality>() {
        
        @Override
        public DefaultFieldCardinality newMessage() {
            return new DefaultFieldCardinality();
        }
        
        @Override
        public Class<? super DefaultFieldCardinality> typeClass() {
            return DefaultFieldCardinality.class;
        }
        
        @Override
        public String messageName() {
            return DefaultFieldCardinality.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return DefaultFieldCardinality.class.getName();
        }
        
        @Override
        public boolean isInitialized(DefaultFieldCardinality message) {
            return true;
        }
        
        @Override
        public void writeTo(Output output, DefaultFieldCardinality message) throws IOException {
            output.writeString(1, message.columnVisibility, false);
            
            if (message.columnVisibility != null)
                output.writeString(2, message.columnVisibility, false);
            
            output.writeUInt64(3, message.cardinality, false);
            output.writeString(4, message.lower, false);
            output.writeString(5, message.upper, false);
        }
        
        @Override
        public void mergeFrom(Input input, DefaultFieldCardinality message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.field = input.readString();
                        break;
                    case 2:
                        message.columnVisibility = input.readString();
                        break;
                    case 3:
                        message.cardinality = input.readUInt64();
                        break;
                    case 4:
                        message.lower = input.readString();
                        break;
                    case 5:
                        message.upper = input.readString();
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
                    return "field";
                case 2:
                    return "columnVisibility";
                case 3:
                    return "cardinality";
                case 4:
                    return "lower";
                case 5:
                    return "upper";
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }
        
        private final HashMap<String,Integer> fieldMap = new HashMap<String,Integer>();
        {
            fieldMap.put("field", 1);
            fieldMap.put("columnVisibility", 2);
            fieldMap.put("cardinality", 3);
            fieldMap.put("lower", 4);
            fieldMap.put("upper", 5);
        }
    };
}
