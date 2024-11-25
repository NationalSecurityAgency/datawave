package datawave.webservice.dictionary.edge;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
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
@XmlType(propOrder = {"edgeType", "edgeRelationship", "edgeAttribute1Source", "eventFields"})
public class DefaultMetadata extends MetadataBase<DefaultMetadata> implements Serializable, Message<DefaultMetadata> {
    
    private static final long serialVersionUID = -1621626861385080614L;
    
    @XmlAttribute(required = true)
    private String edgeType;
    
    // The edge relationship. Will only be
    // one component if this is a stats edge.
    @XmlAttribute(required = true)
    private String edgeRelationship;
    
    // The edge attribute1 source. Will
    // only be one component if this is a stats edge.
    @XmlAttribute
    private String edgeAttribute1Source;
    
    @XmlElementWrapper(name = "EventFields", required = true)
    @XmlElement(name = "EventFields")
    private List<EventField> eventFields;
    
    @XmlAttribute(required = true)
    private String startDate;
    
    @XmlAttribute
    private String jexlPrecondition;
    
    // Last time this Edge Definition was created (helps show how out of date the entry might be)
    @XmlAttribute(required = true)
    private String lastUpdated;
    
    public String getEdgeType() {
        return edgeType;
    }
    
    public void setEdgeType(String edgeType) {
        this.edgeType = edgeType;
    }
    
    public String getEdgeRelationship() {
        return edgeRelationship;
    }
    
    public void setEdgeRelationship(String edgeRelationship) {
        this.edgeRelationship = edgeRelationship;
    }
    
    public String getEdgeAttribute1Source() {
        return edgeAttribute1Source;
    }
    
    public void setEdgeAttribute1Source(String edgeAttribute1Source) {
        this.edgeAttribute1Source = edgeAttribute1Source;
    }
    
    public String getStartDate() {
        return startDate;
    }
    
    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }
    
    public String getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
    
    public String getJexlPrecondition() {
        return jexlPrecondition;
    }
    
    public boolean hasJexlPrecondition() {
        return (getJexlPrecondition() != null);
    }
    
    public boolean hasEdgeAttribute1Source() {
        return !StringUtils.isEmpty(this.edgeAttribute1Source);
    }
    
    public List<EventField> getEventFields() {
        return eventFields;
    }
    
    public void setEventFields(List<EventField> eventFields) {
        this.eventFields = eventFields;
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DefaultMetadata)) {
            return false;
        } else {
            DefaultMetadata other = (DefaultMetadata) o;
            return Objects.equal(other.getEdgeType(), this.getEdgeType()) && Objects.equal(other.getEdgeRelationship(), this.getEdgeRelationship())
                            && Objects.equal(other.getEdgeAttribute1Source(), this.getEdgeAttribute1Source())
                            && Objects.equal(other.getStartDate(), this.getStartDate()) && Objects.equal(other.getLastUpdated(), this.getLastUpdated());
        }
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(this.getEdgeType(), this.getEdgeRelationship(), this.getEdgeAttribute1Source(), this.getStartDate(), this.getLastUpdated());
    }
    
    //
    // protostuff
    //
    public static Schema<DefaultMetadata> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<DefaultMetadata> cachedSchema() {
        return SCHEMA;
    }
    
    private enum METAFIELD_BASE implements FieldAccessor {
        EDGE_TYPE(1, "edgeType"),
        EDGE_RELATIONSHIP(2, "edgeRelationship"),
        EDGE_ATTRIBUTE1(3, "edgeAttribute1Source"),
        EVENT_FIELDS(4, "eventFields"),
        START_DATE(5, "startDate"),
        LAST_UPDATED(6, "lastUpdated"),
        UNKNOWN(0, "UNKNOWN");
        
        final int fn;
        final String name;
        
        METAFIELD_BASE(int fieldNumber, String fieldName) {
            this.fn = fieldNumber;
            this.name = fieldName;
        }
        
        public int getFieldNumber() {
            return fn;
        }
        
        public String getFieldName() {
            return name;
        }
    }
    
    private static final ProtostuffField<METAFIELD_BASE> FIELD = new ProtostuffField<>(METAFIELD_BASE.class);
    
    @XmlTransient
    private static final Schema<DefaultMetadata> SCHEMA = new Schema<DefaultMetadata>() {
        
        @Override
        public DefaultMetadata newMessage() {
            return new DefaultMetadata();
        }
        
        @Override
        public Class<? super DefaultMetadata> typeClass() {
            return DefaultMetadata.class;
        }
        
        @Override
        public String messageName() {
            return DefaultMetadata.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return DefaultMetadata.class.getName();
        }
        
        @Override
        public boolean isInitialized(DefaultMetadata message) {
            return true;
        }
        
        @Override
        public void writeTo(Output output, DefaultMetadata message) throws IOException {
            if (message.edgeType != null)
                output.writeString(METAFIELD_BASE.EDGE_TYPE.getFieldNumber(), message.edgeType, false);
            if (message.edgeRelationship != null)
                output.writeString(METAFIELD_BASE.EDGE_RELATIONSHIP.getFieldNumber(), message.edgeRelationship, false);
            if (message.edgeAttribute1Source != null)
                output.writeString(METAFIELD_BASE.EDGE_ATTRIBUTE1.getFieldNumber(), message.edgeAttribute1Source, false);
            if (message.eventFields != null) {
                for (EventField field : message.getEventFields()) {
                    output.writeObject(METAFIELD_BASE.EVENT_FIELDS.getFieldNumber(), field, EventField.getSchema(), true);
                }
            }
            if (message.startDate != null) {
                output.writeString(METAFIELD_BASE.START_DATE.getFieldNumber(), message.startDate, false);
            }
            if (message.lastUpdated != null) {
                output.writeString(METAFIELD_BASE.LAST_UPDATED.getFieldNumber(), message.lastUpdated, false);
            }
        }
        
        @Override
        public void mergeFrom(Input input, DefaultMetadata message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (FIELD.parseFieldNumber(number)) {
                    case EDGE_TYPE:
                        message.edgeType = input.readString();
                        break;
                    case EDGE_RELATIONSHIP:
                        message.edgeRelationship = input.readString();
                        break;
                    case EDGE_ATTRIBUTE1:
                        message.edgeAttribute1Source = input.readString();
                        break;
                    case EVENT_FIELDS:
                        if (message.eventFields == null) {
                            message.eventFields = new ArrayList<>();
                        }
                        message.eventFields.add(input.mergeObject(null, EventField.getSchema()));
                        break;
                    case START_DATE:
                        message.startDate = input.readString();
                        break;
                    case LAST_UPDATED:
                        message.lastUpdated = input.readString();
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
            METAFIELD_BASE field = FIELD.parseFieldNumber(number);
            if (field == METAFIELD_BASE.UNKNOWN) {
                return null;
            }
            return field.getFieldName();
        }
        
        @Override
        public int getFieldNumber(String name) {
            METAFIELD_BASE field = FIELD.parseFieldName(name);
            return field.getFieldNumber();
        }
        
    };
    
}
