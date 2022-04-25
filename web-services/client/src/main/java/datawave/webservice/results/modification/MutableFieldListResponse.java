package datawave.webservice.results.modification;

import java.io.Serializable;
import java.util.Set;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "MutableFieldListResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class MutableFieldListResponse implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlAttribute(name = "Datatype", required = true)
    private String datatype = null;
    
    @XmlElementWrapper(name = "MutableFields")
    @XmlElement(name = "Field")
    private Set<String> mutableFields = null;
    
    public String getDatatype() {
        return datatype;
    }
    
    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }
    
    public Set<String> getMutableFields() {
        return mutableFields;
    }
    
    public void setMutableFields(Set<String> mutableFields) {
        this.mutableFields = mutableFields;
    }
    
}
