package datawave.webservice.results.modification;

import java.io.Serializable;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

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
