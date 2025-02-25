package datawave.webservice.response.objects;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import datawave.webservice.query.util.TypedValue;

@XmlAccessorType(XmlAccessType.NONE)
public class Entry implements Serializable {
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "Key")
    private KeyBase key;
    
    @XmlElement(name = "Value")
    private TypedValue value;
    
    public Entry() {};
    
    public Entry(KeyBase key, Object value) {
        this.key = key;
        this.value = new TypedValue(value);
    }
    
    public KeyBase getKey() {
        return key;
    }
    
    public Object getValue() {
        return this.value.getValue();
    }
    
}
