package datawave.webservice.modification;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

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
