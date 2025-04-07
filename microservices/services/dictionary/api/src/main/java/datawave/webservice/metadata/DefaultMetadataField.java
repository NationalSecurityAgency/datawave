package datawave.webservice.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import datawave.webservice.dictionary.data.DefaultDescription;
import datawave.webservice.dictionary.data.DescriptionBase;
import datawave.webservice.query.result.event.MapSchema;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import lombok.Data;

@Data
@XmlAccessorType(XmlAccessType.NONE)
@XmlType(propOrder = {"fieldName", "internalFieldName", "dataType", "descriptions", "indexOnly", "forwardIndexed", "reverseIndexed", "normalized", "tokenized",
        "types"})
public class DefaultMetadataField extends MetadataFieldBase<DefaultMetadataField,DefaultDescription> implements Serializable, Message<DefaultMetadataField> {
    private static final long serialVersionUID = 2050632989270455091L;
    
    @XmlAttribute(required = true)
    private String fieldName;
    
    @XmlAttribute
    private String internalFieldName;
    
    @XmlAttribute(required = true)
    private String dataType;
    
    @XmlAttribute
    private Boolean indexOnly = false;
    
    @XmlAttribute
    private Boolean forwardIndexed = false;
    
    @XmlAttribute
    private Boolean normalized = false;
    
    @XmlAttribute
    private Boolean reverseIndexed = false;
    
    @XmlAttribute
    private Boolean tokenized = false;
    
    @XmlElementWrapper(name = "Types")
    @XmlElement(name = "Types")
    private List<String> types;
    
    @XmlElementWrapper(name = "Descriptions")
    @XmlElement(name = "Description")
    private Set<DefaultDescription> descriptions = new HashSet<>();
    
    @XmlAttribute
    private String lastUpdated;
    
    public Boolean isIndexOnly() {
        return indexOnly == null ? false : indexOnly;
    }
    
    public Boolean isForwardIndexed() {
        return forwardIndexed;
    }
    
    public Boolean isReverseIndexed() {
        return reverseIndexed;
    }
    
    public Boolean isNormalized() {
        return normalized;
    }
    
    public Boolean isTokenized() {
        return tokenized;
    }
    
    public void addType(String type) {
        if (types == null)
            types = new ArrayList<>();
        
        types.add(type);
    }
    
    public void setDescription(Collection<DefaultDescription> descriptions) {
        this.descriptions = new HashSet<>(descriptions);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(6197, 7993).append(this.fieldName).hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        
        DefaultMetadataField field = (DefaultMetadataField) o;
        
        return new EqualsBuilder().append(fieldName, field.fieldName).append(internalFieldName, field.internalFieldName).append(dataType, field.dataType)
                        .append(indexOnly, field.indexOnly).append(forwardIndexed, field.forwardIndexed).append(normalized, field.normalized)
                        .append(reverseIndexed, field.reverseIndexed).append(tokenized, field.tokenized).append(types, field.types)
                        .append(descriptions, field.descriptions).append(lastUpdated, field.lastUpdated).isEquals();
    }
    
    @SuppressWarnings("unused")
    public static Schema<DefaultMetadataField> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<DefaultMetadataField> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<DefaultMetadataField> SCHEMA = new Schema<DefaultMetadataField>() {
        private final HashMap<String,Integer> fieldMap = new HashMap<>();
        {
            fieldMap.put("fieldName", 1);
            fieldMap.put("internalFieldName", 2);
            fieldMap.put("dataType", 3);
            fieldMap.put("indexOnly", 4);
            fieldMap.put("indexed", 5);
            fieldMap.put("reverseIndexed", 6);
            fieldMap.put("normalized", 7);
            fieldMap.put("types", 8);
            fieldMap.put("descriptions", 9);
            fieldMap.put("lastUpdated", 10);
            fieldMap.put("tokenized", 11);
        }
        
        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "fieldName";
                case 2:
                    return "internalFieldName";
                case 3:
                    return "dataType";
                case 4:
                    return "indexOnly";
                case 5:
                    return "indexed";
                case 6:
                    return "reverseIndexed";
                case 7:
                    return "normalized";
                case 8:
                    return "types";
                case 9:
                    return "descriptions";
                case 10:
                    return "lastUpdated";
                case 11:
                    return "tokenized";
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }
        
        @Override
        public boolean isInitialized(DefaultMetadataField message) {
            return true;
        }
        
        @Override
        public DefaultMetadataField newMessage() {
            return new DefaultMetadataField();
        }
        
        @Override
        public String messageName() {
            return DefaultMetadataField.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return DefaultMetadataField.class.getName();
        }
        
        @Override
        public Class<? super DefaultMetadataField> typeClass() {
            return DefaultMetadataField.class;
        }
        
        @Override
        public void mergeFrom(Input input, DefaultMetadataField message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.fieldName = input.readString();
                        break;
                    case 2:
                        message.internalFieldName = input.readString();
                        break;
                    case 3:
                        message.dataType = input.readString();
                        break;
                    case 4:
                        message.indexOnly = input.readBool();
                        break;
                    case 5:
                        message.forwardIndexed = input.readBool();
                        break;
                    case 6:
                        message.reverseIndexed = input.readBool();
                        break;
                    case 7:
                        message.normalized = input.readBool();
                        break;
                    case 8:
                        if (null == message.types) {
                            message.types = new ArrayList<>();
                        }
                        message.types.add(input.readString());
                        break;
                    case 9:
                        int size = input.readInt32();
                        message.descriptions = new HashSet<>(size);
                        for (int i = 0; i < size; i++) {
                            Map<String,String> markings = new HashMap<>();
                            input.mergeObject(markings, MapSchema.SCHEMA);
                            message.descriptions.add(new DefaultDescription(input.readString(), markings));
                        }
                        break;
                    case 10:
                        message.lastUpdated = input.readString();
                        break;
                    case 11:
                        message.tokenized = input.readBool();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }
        
        @Override
        public void writeTo(Output output, DefaultMetadataField message) throws IOException {
            if (message.fieldName != null) {
                output.writeString(1, message.fieldName, false);
            }
            
            if (message.internalFieldName != null) {
                output.writeString(2, message.internalFieldName, false);
            }
            
            if (message.dataType != null) {
                output.writeString(3, message.dataType, false);
            }
            
            output.writeBool(4, message.indexOnly, false);
            output.writeBool(5, message.forwardIndexed, false);
            output.writeBool(6, message.reverseIndexed, false);
            output.writeBool(7, message.normalized, false);
            
            if (message.types != null) {
                for (String typeClass : message.types)
                    output.writeString(8, typeClass, true);
            }
            
            output.writeInt32(9, message.getDescriptions().size(), false);
            for (DescriptionBase desc : message.getDescriptions()) {
                output.writeString(9, desc.getDescription(), true);
                output.writeObject(9, desc.getMarkings(), MapSchema.SCHEMA, false);
            }
            output.writeString(10, message.lastUpdated, false);
            output.writeBool(11, message.tokenized, false);
        }
        
    };
    
    @Override
    public String toString() {
        return "MetadataField [fieldName=" + fieldName + ", internalFieldName=" + internalFieldName + ",dataType=" + dataType + ", descriptions= "
                        + descriptions + ", indexOnly=" + indexOnly + ", indexed=" + forwardIndexed + ", reverseIndexed=" + reverseIndexed + ", normalized="
                        + normalized + ", tokenized=" + tokenized + ", types=" + types + ", lastUpdated=" + lastUpdated + "]";
    }
}
