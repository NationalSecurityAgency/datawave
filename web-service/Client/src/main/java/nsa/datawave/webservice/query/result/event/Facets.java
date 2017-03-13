package nsa.datawave.webservice.query.result.event;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import nsa.datawave.webservice.query.data.ObjectSizeOf;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlTransient;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Facets extends FacetsBase implements Serializable, Message<Facets>, ObjectSizeOf {
    
    private static final long serialVersionUID = 1L;
    
    public void setMarkings(Map<String,String> markings) {
        this.markings = markings;
    }
    
    public Map<String,String> getMarkings() {
        return this.markings;
    }
    
    @Override
    public String toString() {
        return this.fields.toString();
    }
    
    public static Schema<Facets> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<Facets> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<Facets> SCHEMA = new Schema<Facets>() {
        public Facets newMessage() {
            return new Facets();
        }
        
        public Class<Facets> typeClass() {
            return Facets.class;
        }
        
        public String messageName() {
            return Facets.class.getSimpleName();
        }
        
        public String messageFullName() {
            return Facets.class.getName();
        }
        
        public boolean isInitialized(Facets message) {
            return true;
        }
        
        public void writeTo(Output output, Facets message) throws IOException {
            if (message.fields != null) {
                for (FieldCardinality field : message.fields) {
                    if (field != null)
                        output.writeObject(1, field, FieldCardinality.getSchema(), true);
                }
            }
        }
        
        public void mergeFrom(Input input, Facets message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        if (message.fields == null)
                            message.fields = new ArrayList<FieldCardinality>();
                        FieldCardinality f = input.mergeObject(null, FieldCardinality.getSchema());
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
            fieldMap.put("fields", 1);
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
