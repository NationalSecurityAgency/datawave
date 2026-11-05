package datawave.webservice.response.objects;

import java.io.Serializable;

import datawave.webservice.query.util.TypedValue;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;

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
