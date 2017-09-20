package datawave.webservice.response.objects;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class UserPermissions implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "SystemPermission")
    private List<SystemPermission> systemPermissions = null;
    
    @XmlElement(name = "TablePermission")
    private List<TablePermission> tablePermissions = null;
    
    @XmlElement(name = "NamespacePermission")
    private List<NamespacePermission> namespacePermissions = null;
    
    public UserPermissions() {
        
    }
    
    public UserPermissions(List<SystemPermission> systemPermissions, List<TablePermission> tablePermissions, List<NamespacePermission> namespacePermissions) {
        this.systemPermissions = systemPermissions;
        this.tablePermissions = tablePermissions;
        this.namespacePermissions = namespacePermissions;
    }
    
    public List<SystemPermission> getSystemPermissions() {
        return systemPermissions;
    }
    
    public List<TablePermission> getTablePermissions() {
        return tablePermissions;
    }
    
    public List<NamespacePermission> getNamespacePermissions() {
        return namespacePermissions;
    }
    
    public void setSystemPermissions(List<SystemPermission> systemPermissions) {
        this.systemPermissions = systemPermissions;
    }
    
    public void setTablePermissions(List<TablePermission> tablePermissions) {
        this.tablePermissions = tablePermissions;
    }
    
    public void setNamespacePermissions(List<NamespacePermission> namespacePermissions) {
        this.namespacePermissions = namespacePermissions;
    }
    
}
