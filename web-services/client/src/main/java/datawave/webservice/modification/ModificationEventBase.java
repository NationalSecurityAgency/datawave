package datawave.webservice.modification;

import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public abstract class ModificationEventBase<T extends ModificationOperation> {
    
    @XmlElement(name = "id", required = true)
    protected String id = null;
    @XmlElement(name = "idType", required = true)
    protected String idType = null;
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getIdType() {
        return idType;
    }
    
    public void setIdType(String idType) {
        this.idType = idType;
    }
    
    public abstract List<T> getOperations();
    
    public abstract void setOperations(List<T> operations);
    
}
