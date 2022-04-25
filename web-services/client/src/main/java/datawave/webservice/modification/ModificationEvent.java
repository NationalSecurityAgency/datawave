package datawave.webservice.modification;

import java.io.Serializable;
import java.util.List;
import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;

@XmlRootElement(name = "ModificationEvent")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ModificationEvent extends ModificationEventBase<ModificationOperationImpl> implements Serializable {
    
    private static final long serialVersionUID = 6L;
    
    @XmlElement(name = "user", required = true)
    private String user = null;
    
    @XmlElementWrapper(name = "operations", required = true)
    @XmlElement(name = "operation", required = true)
    protected List<ModificationOperationImpl> operations = null;
    
    public String getUser() {
        return user;
    }
    
    public void setUser(String user) {
        this.user = user;
    }
    
    public List<ModificationOperationImpl> getOperations() {
        return operations;
    }
    
    public void setOperations(List<ModificationOperationImpl> operations) {
        this.operations = operations;
    }
    
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("id", id);
        tsb.append("idType", idType);
        tsb.append("operations", operations);
        tsb.append("user", user);
        return tsb.toString();
    }
}
