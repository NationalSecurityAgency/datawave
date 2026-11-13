package datawave.webservice.response.objects;

import java.io.Serializable;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlValue;

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
