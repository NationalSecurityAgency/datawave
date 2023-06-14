package datawave.webservice.modification;

import org.apache.commons.lang.builder.ToStringBuilder;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement(name = "ModificationOperationImpl")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ModificationOperationImpl extends ModificationOperation<ModificationOperationImpl> implements Serializable {

    private static final long serialVersionUID = 5L;

    @XmlElement(name = "operationMode", required = true)
    protected OPERATIONMODE operationMode = null;
    @XmlElement(name = "fieldName", required = true)
    protected String fieldName = null;
    @XmlElement(name = "fieldValue", required = true)
    protected String fieldValue = null;
    @XmlElement(name = "oldFieldValue", required = false)
    protected String oldFieldValue = null;
    @XmlElement(name = "columnVisibility")
    protected String columnVisibility = null;
    @XmlElement(name = "oldColumnVisibility")
    protected String oldColumnVisibility = null;

    public OPERATIONMODE getOperationMode() {
        return operationMode;
    }

    public void setOperationMode(OPERATIONMODE operationMode) {
        this.operationMode = operationMode;
    }

    public void setOperationMode(String operationMode) {
        this.operationMode = OPERATIONMODE.valueOf(operationMode);
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }

    public String getOldFieldValue() {
        return oldFieldValue;
    }

    public void setOldFieldValue(String oldFieldValue) {
        this.oldFieldValue = oldFieldValue;
    }

    public String getColumnVisibility() {
        return columnVisibility;
    }

    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public String getOldColumnVisibility() {
        return oldColumnVisibility;
    }

    public void setOldColumnVisibility(String oldColumnVisibility) {
        this.oldColumnVisibility = oldColumnVisibility;
    }

    public ModificationOperationImpl clone() {
        ModificationOperationImpl modOp = new ModificationOperationImpl();
        modOp.setOperationMode(operationMode);
        modOp.setFieldName(fieldName);
        modOp.setFieldValue(fieldValue);
        modOp.setOldFieldValue(oldFieldValue);
        modOp.setColumnVisibility(columnVisibility);
        modOp.setOldColumnVisibility(oldColumnVisibility);
        return modOp;
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("operationMode", operationMode);
        tsb.append("fieldName", fieldName);
        tsb.append("fieldValue", fieldValue);
        tsb.append("oldFieldValue", oldFieldValue);
        tsb.append("columnVisibility", columnVisibility);
        tsb.append("oldColumnVisibility", oldColumnVisibility);
        return tsb.toString();
    }
}
