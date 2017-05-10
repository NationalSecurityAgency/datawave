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

import datawave.webservice.query.result.event.FieldCardinalityBase;

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
public class FieldCardinality extends FieldCardinalityBase implements Serializable, Message<FieldCardinality> {
    
    private static final long serialVersionUID = 1L;
    
    @XmlAttribute(name = "columnVisibility")
    private String columnVisibility;
    @XmlAttribute(name = "lower")
    private String lower;
    @XmlAttribute(name = "upper")
    private String upper;
    @XmlElement(name = "cardinality")
    private Long cardinality;
    
    public FieldCardinality() {}
    
    public FieldCardinality(String columnVisibility, String lower, String upper, Long cardinality) {
        super();
        this.columnVisibility = columnVisibility;
        this.lower = lower;
        this.upper = upper;
        this.cardinality = cardinality;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.query.result.event.FieldCardinalityInterface#setMarkings(java.util.Map)
     */
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
        StringBuilder buf = new StringBuilder();
        buf.append(" columnVisibility=").append(columnVisibility);
        buf.append(" cardinality=").append(cardinality);
        buf.append(" lower=").append(lower);
        buf.append(" upper= ").append(upper).append("] ");
        
        return buf.toString();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(columnVisibility).append(cardinality).append(lower).append(upper).hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof FieldCardinality) {
            FieldCardinality v = (FieldCardinality) o;
            
            EqualsBuilder eb = new EqualsBuilder();
            
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
    
    public static Schema<FieldCardinality> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<FieldCardinality> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<FieldCardinality> SCHEMA = new Schema<FieldCardinality>() {
        
        @Override
        public FieldCardinality newMessage() {
            return new FieldCardinality();
        }
        
        @Override
        public Class<? super FieldCardinality> typeClass() {
            return FieldCardinality.class;
        }
        
        @Override
        public String messageName() {
            return FieldCardinality.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return FieldCardinality.class.getName();
        }
        
        @Override
        public boolean isInitialized(FieldCardinality message) {
            return true;
        }
        
        @Override
        public void writeTo(Output output, FieldCardinality message) throws IOException {
            if (message.columnVisibility != null)
                output.writeString(1, message.columnVisibility, false);
            output.writeUInt64(2, message.cardinality, false);
            output.writeString(3, message.lower, false);
            output.writeString(4, message.upper, false);
        }
        
        @Override
        public void mergeFrom(Input input, FieldCardinality message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.columnVisibility = input.readString();
                        break;
                    case 2:
                        message.cardinality = input.readUInt64();
                        break;
                    case 3:
                        message.lower = input.readString();
                        break;
                    case 4:
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
                    return "columnVisibility";
                case 2:
                    return "cardinality";
                case 3:
                    return "lower";
                case 4:
                    return "upper";
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
            fieldMap.put("columnVisibility", 1);
            fieldMap.put("cardinality", 2);
            fieldMap.put("lower", 3);
            fieldMap.put("upper", 4);
        }
    };
    
    public String getColumnVisibility() {
        return columnVisibility;
    }
    
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
}
