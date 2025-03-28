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
public class TablePermission implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public enum TablePermissionType {
        READ, WRITE, BULK_IMPORT, ALTER_TABLE, GRANT, DROP_TABLE, GET_SUMMARIES
    };
    
    @XmlAttribute
    private String tableName = null;
    
    @XmlValue
    private TablePermissionType permission = null;
    
    public TablePermission() {
        
    }
    
    public TablePermission(String tableName, String permission) {
        this.tableName = tableName;
        this.permission = TablePermissionType.valueOf(permission);
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public TablePermissionType getPermission() {
        return permission;
    }
    
    public void setPermission(TablePermissionType permission) {
        this.permission = permission;
    }
    
    public void setPermission(String permission) {
        this.permission = TablePermissionType.valueOf(permission);
    }
}
