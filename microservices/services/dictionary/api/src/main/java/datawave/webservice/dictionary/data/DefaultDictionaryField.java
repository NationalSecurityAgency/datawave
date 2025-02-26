package datawave.webservice.dictionary.data;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import com.google.common.collect.Sets;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultDictionaryField extends DictionaryFieldBase<DefaultDictionaryField,DefaultDescription> implements Message<DefaultDictionaryField> {
    @XmlElement(name = "fieldName")
    private String fieldName;
    
    @XmlElement(name = "datatype")
    private String datatype;
    
    @XmlElement(name = "descriptions")
    private Set<DefaultDescription> descriptions;
    
    public DefaultDictionaryField() {}
    
    public DefaultDictionaryField(String fieldName, String datatype, DefaultDescription desc) {
        this(fieldName, datatype, Collections.singleton(desc));
    }
    
    public DefaultDictionaryField(String fieldName, String datatype, Collection<DefaultDescription> descs) {
        this.fieldName = fieldName;
        this.datatype = datatype;
        this.descriptions = Sets.newHashSet(descs);
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
    
    public String getDatatype() {
        return datatype;
    }
    
    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }
    
    public Set<DefaultDescription> getDescriptions() {
        return descriptions;
    }
    
    public void addDescription(DefaultDescription description) {
        this.descriptions.add(description);
    }
    
    public void setDescriptions(Set<DefaultDescription> descriptions) {
        this.descriptions = descriptions;
    }
    
    public static Schema<DefaultDictionaryField> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public String toString() {
        return getFieldName() + ", " + getDatatype() + ", " + getDescriptions();
    }
    
    @Override
    /*
     * (non-Javadoc)
     * 
     * @see io.protostuff.Message#cachedSchema()
     */
    public Schema<DefaultDictionaryField> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<DefaultDictionaryField> SCHEMA = new Schema<DefaultDictionaryField>() {
        public DefaultDictionaryField newMessage() {
            return new DefaultDictionaryField();
        }
        
        public Class<DefaultDictionaryField> typeClass() {
            return DefaultDictionaryField.class;
        }
        
        public String messageName() {
            return DefaultDictionaryField.class.getSimpleName();
        }
        
        public String messageFullName() {
            return DefaultDictionaryField.class.getName();
        }
        
        public boolean isInitialized(DefaultDictionaryField message) {
            return true;
        }
        
        public void writeTo(Output output, DefaultDictionaryField message) throws IOException {
            if (message.fieldName != null) {
                output.writeString(1, message.fieldName, false);
            }
            
            if (message.datatype != null) {
                output.writeString(2, message.datatype, false);
            }
            
            if (message.descriptions != null && !message.descriptions.isEmpty()) {
                output.writeInt32(3, message.descriptions.size(), false);
                for (DescriptionBase desc : message.descriptions) {
                    output.writeObject(3, (DefaultDescription) desc, DefaultDescription.getSchema(), true);
                }
            }
        }
        
        public void mergeFrom(Input input, DefaultDictionaryField message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.fieldName = input.readString();
                        break;
                    case 2:
                        message.datatype = input.readString();
                        break;
                    case 3:
                        int count = input.readInt32();
                        message.descriptions = Sets.newHashSet();
                        for (int i = 0; i < count; i++) {
                            message.descriptions.add(input.mergeObject(null, DefaultDescription.getSchema()));
                        }
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
                    return "fieldName";
                case 2:
                    return "datatype";
                case 3:
                    return "description";
                default:
                    return null;
            }
        }
        
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }
        
        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<>();
        {
            fieldMap.put("fieldName", 1);
            fieldMap.put("datatype", 2);
            fieldMap.put("description", 3);
        }
    };
}
