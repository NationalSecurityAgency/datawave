package datawave.webservice.modification;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlSeeAlso;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(ModificationOperationImpl.class)
public abstract class ModificationOperation<T extends ModificationOperation> implements Cloneable {

    @XmlEnum(String.class)
    public enum OPERATIONMODE {
        INSERT, UPDATE, DELETE, REPLACE, KEEP
    }

    public abstract OPERATIONMODE getOperationMode();

    public abstract void setOperationMode(OPERATIONMODE operationMode);

    public abstract void setOperationMode(String operationMode);

    public abstract String getFieldName();

    public abstract void setFieldName(String fieldName);

    public abstract String getFieldValue();

    public abstract void setFieldValue(String fieldValue);

    public abstract String getOldFieldValue();

    public abstract void setOldFieldValue(String oldFieldValue);

    public abstract String getColumnVisibility();

    public abstract void setColumnVisibility(String columnVisibility);

    public abstract String getOldColumnVisibility();

    public abstract void setOldColumnVisibility(String oldColumnVisibility);

    public abstract T clone();
}
