package datawave.webservice.response.objects;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class TableProperty {
    
    @XmlAttribute
    private String name = null;
    
    @XmlValue
    private String value = null;
    
    public TableProperty() {
        
    }
    
    public TableProperty(String name, String value) {
        this.name = name;
        this.value = value;
    }
    
    public String getName() {
        return name;
    }
    
    public String getValue() {
        return value;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
    
}
