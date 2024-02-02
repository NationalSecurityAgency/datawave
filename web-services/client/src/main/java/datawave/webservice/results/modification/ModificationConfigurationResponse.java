package datawave.webservice.results.modification;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "ModificationConfigurationResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ModificationConfigurationResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    @XmlAttribute(name = "name", required = true)
    private String name = null;

    @XmlElement(name = "Description")
    private String description = null;

    @XmlElement(name = "RequestClass")
    private String requestClass = null;

    @XmlElementWrapper(name = "AuthorizedRoles")
    @XmlElement(name = "Role")
    private List<String> authorizedRoles = null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getRequestClass() {
        return requestClass;
    }

    public void setRequestClass(String requestClass) {
        this.requestClass = requestClass;
    }

    public List<String> getAuthorizedRoles() {
        return authorizedRoles;
    }

    public void setAuthorizedRoles(List<String> authorizedRoles) {
        this.authorizedRoles = authorizedRoles;
    }

}
