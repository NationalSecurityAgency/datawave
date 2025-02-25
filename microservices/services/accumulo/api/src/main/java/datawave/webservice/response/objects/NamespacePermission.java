package datawave.webservice.response.objects;

import java.io.Serializable;

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
public class NamespacePermission implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public enum NamespacePermissionType {
        READ, WRITE, ALTER_NAMESPACE, GRANT, ALTER_TABLE, CREATE_TABLE, DROP_TABLE, BULK_IMPORT, DROP_NAMESPACE
    }
    
    @XmlAttribute
    private String namespaceName = null;
    
    @XmlValue
    private NamespacePermissionType permission = null;
    
    public NamespacePermission() {
        
    }
    
    public NamespacePermission(String namespace, String permission) {
        this.namespaceName = namespace;
        this.permission = NamespacePermissionType.valueOf(permission);
    }
    
    public String getNamespaceName() {
        return namespaceName;
    }
    
    public void setNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
    }
    
    public NamespacePermissionType getPermission() {
        return permission;
    }
    
    public void setPermission(NamespacePermissionType permission) {
        this.permission = permission;
    }
    
    public void setPermission(String permission) {
        this.permission = NamespacePermissionType.valueOf(permission);
    }
}
