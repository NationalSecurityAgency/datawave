package datawave.webservice.dictionary.edge;

import java.io.IOException;
import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Objects;

import datawave.webservice.query.result.util.protostuff.FieldAccessor;
import datawave.webservice.query.result.util.protostuff.ProtostuffField;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlType(propOrder = {"sourceField", "sinkField", "enrichmentField", "enrichmentIndex", "jexlPrecondition"})
public class EventField implements Serializable, Message<EventField> {
    
    private static final long serialVersionUID = -1133131083937142620L;
    
    private enum FIELD_BASE implements FieldAccessor {
        SOURCE(1, "sourceField"), SINK(2, "sinkField"), ENRICHMENT(3, "enrichmentField"), ENRICHMENT_INDEX(4, "enrichmentIndex"), UNKNOWN(0, "UNKNOWN");
        
        final int fn;
        final String name;
        
        FIELD_BASE(int fn, String name) {
            this.fn = fn;
            this.name = name;
        }
        
        public int getFieldNumber() {
            return fn;
        }
        
        public String getFieldName() {
            return name;
        }
    }
    
    private static final ProtostuffField<FIELD_BASE> FIELD = new ProtostuffField<>(FIELD_BASE.class);
    
    @XmlAttribute(required = true)
    private String sourceField;
    
    @XmlAttribute(required = true)
    private String sinkField;
    
    @XmlAttribute
    private String enrichmentField;
    
    @XmlAttribute
    private String enrichmentIndex;
    
    @XmlAttribute
    private String jexlPrecondition;
    
    //
    // Getters/Setters
    //
    public String getSourceField() {
        return sourceField;
    }
    
    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }
    
    public String getSinkField() {
        return sinkField;
    }
    
    public void setSinkField(String sinkField) {
        this.sinkField = sinkField;
    }
    
    public String getEnrichmentField() {
        return enrichmentField;
    }
    
    public void setEnrichmentField(String enrichmentField) {
        this.enrichmentField = enrichmentField;
    }
    
    public boolean hasEnrichment() {
        return !StringUtils.isEmpty(this.enrichmentField) && !StringUtils.isEmpty(this.enrichmentIndex);
    }
    
    public String getEnrichmentIndex() {
        return enrichmentIndex;
    }
    
    public void setEnrichmentIndex(String enrichmentIndex) {
        this.enrichmentIndex = enrichmentIndex;
    }
    
    public boolean hasJexlPrecondition() {
        return (getJexlPrecondition() != null);
    }
    
    public String getJexlPrecondition() {
        return jexlPrecondition;
    }
    
    public void setJexlPrecondition(String jexlPrecondition) {
        this.jexlPrecondition = jexlPrecondition;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(this.sourceField).append(",").append(this.sinkField);
        /*
         * if (this.enrichmentField == null && this.jexlPrecondition == null) { sb.append("]"); } else if (this.enrichmentField != null && this.jexlPrecondition
         * == null) { sb.append(" | ").append(this.enrichmentField).append("=").append(this.enrichmentIndex).append("]"); } else if (this.enrichmentField ==
         * null && this.jexlPrecondition != null) { sb.append(" | ").append(this.jexlPrecondition).append("]"); }
         * 
         * else { sb.append(" | ").append(this.enrichmentField).append("=").append(this.enrichmentIndex);
         * sb.append(" | ").append(this.jexlPrecondition).append("]"); }
         */
        
        sb.append(" | ");
        
        if (this.enrichmentField != null) {
            sb.append(this.enrichmentField).append("=").append(this.enrichmentIndex);
        }
        
        sb.append(" | ");
        
        if (this.jexlPrecondition != null) {
            sb.append(this.jexlPrecondition);
        }
        
        sb.append("]");
        
        return sb.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EventField)) {
            return false;
        } else {
            EventField other = (EventField) o;
            return Objects.equal(other.getSourceField(), this.getSourceField()) && Objects.equal(other.getSinkField(), this.getSinkField())
                            && Objects.equal(other.getEnrichmentField(), this.getEnrichmentField())
                            && Objects.equal(other.getEnrichmentIndex(), this.getEnrichmentIndex());
        }
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(this.getSourceField(), this.getSinkField(), this.getEnrichmentField(), this.getEnrichmentIndex());
    }
    
    //
    // protostuff
    //
    @Override
    public Schema<EventField> cachedSchema() {
        return SCHEMA;
    }
    
    public static Schema<EventField> getSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<EventField> SCHEMA = new Schema<EventField>() {
        
        @Override
        public EventField newMessage() {
            return new EventField();
        }
        
        @Override
        public Class<? super EventField> typeClass() {
            return EventField.class;
        }
        
        @Override
        public String messageName() {
            return EventField.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return EventField.class.getName();
        }
        
        @Override
        public boolean isInitialized(EventField message) {
            return true;
        }
        
        @Override
        public void writeTo(Output output, EventField message) throws IOException {
            if (message.sourceField != null)
                output.writeString(FIELD_BASE.SOURCE.getFieldNumber(), message.sourceField, false);
            if (message.sinkField != null)
                output.writeString(FIELD_BASE.SINK.getFieldNumber(), message.sinkField, false);
            if (message.enrichmentField != null)
                output.writeString(FIELD_BASE.ENRICHMENT.getFieldNumber(), message.enrichmentField, false);
            if (message.enrichmentIndex != null)
                output.writeString(FIELD_BASE.ENRICHMENT_INDEX.getFieldNumber(), message.enrichmentIndex, false);
        }
        
        @Override
        public void mergeFrom(Input input, EventField message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (FIELD.parseFieldNumber(number)) {
                    case SOURCE:
                        message.sourceField = input.readString();
                        break;
                    case SINK:
                        message.sinkField = input.readString();
                        break;
                    case ENRICHMENT:
                        message.enrichmentField = input.readString();
                        break;
                    case ENRICHMENT_INDEX:
                        message.enrichmentIndex = input.readString();
                        break;
                    case UNKNOWN:
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }
        
        @Override
        public String getFieldName(int number) {
            FIELD_BASE field = FIELD.parseFieldNumber(number);
            if (field == FIELD_BASE.UNKNOWN) {
                return null;
            }
            return field.getFieldName();
        }
        
        @Override
        public int getFieldNumber(String name) {
            return FIELD.parseFieldName(name).getFieldNumber();
        }
        
    };
    
}
