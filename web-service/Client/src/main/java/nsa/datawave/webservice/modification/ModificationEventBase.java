package nsa.datawave.webservice.modification;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ModificationEventBase<T extends ModificationOperation> {
    
    @XmlElement(name = "id", required = true)
    protected String id = null;
    @XmlElement(name = "idType", required = true)
    protected String idType = null;
    @XmlElementWrapper(name = "operations", required = true)
    @XmlElement(name = "operation", required = true)
    protected List<T> operations = null;
    
    public ModificationEventBase() {
        super();
    }
    
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
    
    public List<T> getOperations() {
        return operations;
    }
    
    public void setOperations(List<T> operations) {
        this.operations = operations;
    }
    
}
