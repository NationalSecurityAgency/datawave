package datawave.webservice.request.objects;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;

import datawave.webservice.query.util.OptionallyEncodedString;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class MutationEntry {
    
    @XmlElement
    private OptionallyEncodedString colFam = null;
    
    @XmlElement
    private OptionallyEncodedString colQual = null;
    
    @XmlElements(value = {@XmlElement(name = "valueRef", type = ValueReference.class), @XmlElement(name = "value", type = OptionallyEncodedString.class),
            @XmlElement(name = "remove", type = Boolean.class)})
    private Object value = null;
    
    @XmlAttribute(required = true)
    private String visibility = null;
    
    public OptionallyEncodedString getColFam() {
        return colFam;
    }
    
    public OptionallyEncodedString getColQual() {
        return colQual;
    }
    
    public Object getValue() {
        return value;
    }
    
    public String getVisibility() {
        return visibility;
    }
    
    public void setColFam(OptionallyEncodedString colFam) {
        this.colFam = colFam;
    }
    
    public void setColQual(OptionallyEncodedString colQual) {
        this.colQual = colQual;
    }
    
    public void setValue(Object value) {
        this.value = value;
    }
    
    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }
    
}
