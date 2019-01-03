package datawave.webservice.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

@XmlAccessorType(XmlAccessType.NONE)
public class FieldMapping implements Serializable, Comparable<FieldMapping> {
    
    private static final long serialVersionUID = 1L;
    
    @XmlAttribute(name = "datatype", required = true)
    private String datatype = null;
    
    @XmlAttribute(name = "fieldName", required = true)
    private String fieldName = null;
    
    @XmlAttribute(name = "modelFieldName", required = true)
    private String modelFieldName = null;
    
    @XmlAttribute(name = "direction", required = true)
    private Direction direction = null;
    
    @XmlAttribute(name = "columnVisibility", required = true)
    private String columnVisibility = null;
    
    @XmlAttribute(name = "indexOnly", required = false)
    private Boolean indexOnly = false;
    
    public FieldMapping() {
        super();
    }
    
    public FieldMapping(String datatype, String fieldName, String modelFieldName, Direction direction, String columnVisibility) {
        this(datatype, fieldName, modelFieldName, direction, columnVisibility, false);
    }
    
    public FieldMapping(String datatype, String fieldName, String modelFieldName, Direction direction, String columnVisibility, Boolean indexOnly) {
        super();
        this.datatype = datatype;
        this.fieldName = fieldName;
        this.modelFieldName = modelFieldName;
        this.direction = direction;
        this.columnVisibility = columnVisibility;
        this.indexOnly = indexOnly;
    }
    
    public String getDatatype() {
        return datatype;
    }
    
    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
    
    public String getModelFieldName() {
        return modelFieldName;
    }
    
    public void setModelFieldName(String modelFieldName) {
        this.modelFieldName = modelFieldName;
    }
    
    public Direction getDirection() {
        return direction;
    }
    
    public void setDirection(Direction direction) {
        this.direction = direction;
    }
    
    public String getColumnVisibility() {
        return columnVisibility;
    }
    
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
    
    public Boolean getIndexOnly() {
        return indexOnly;
    }
    
    public void setIndexOnly(Boolean indexOnly) {
        this.indexOnly = indexOnly;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(columnVisibility).append(datatype).append(direction).append(fieldName).append(modelFieldName)
                        .append(indexOnly).toHashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (obj.getClass() != this.getClass())
            return false;
        FieldMapping other = (FieldMapping) obj;
        return new EqualsBuilder().append(columnVisibility, other.columnVisibility).append(datatype, other.datatype).append(direction, other.direction)
                        .append(fieldName, other.fieldName).append(modelFieldName, other.modelFieldName).append(indexOnly, other.indexOnly).isEquals();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("columnVisibility", columnVisibility).append("datatype", datatype).append("direction", direction)
                        .append("fieldName", fieldName).append("modelFieldName", modelFieldName).append("index_only", indexOnly).toString();
    }
    
    @Override
    public int compareTo(FieldMapping obj) {
        
        if (obj == null) {
            throw new IllegalArgumentException("can not compare null");
        }
        
        if (obj == this)
            return 0;
        
        return new CompareToBuilder().append(datatype, ((FieldMapping) obj).datatype).append(fieldName, ((FieldMapping) obj).fieldName)
                        .append(modelFieldName, ((FieldMapping) obj).modelFieldName).append(direction, ((FieldMapping) obj).direction)
                        .append(indexOnly, ((FieldMapping) obj).indexOnly).append(columnVisibility, ((FieldMapping) obj).columnVisibility).toComparison();
    }
    
}
