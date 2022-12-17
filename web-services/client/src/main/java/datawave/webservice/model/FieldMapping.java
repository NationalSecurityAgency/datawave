package datawave.webservice.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

@XmlRootElement(name = "FieldMapping")
@XmlAccessorType(XmlAccessType.FIELD)
public class FieldMapping extends Mapping implements Serializable, Comparable<Mapping> {
    
    @XmlAttribute(name = "fieldName", required = true)
    private String fieldName = null;
    
    public FieldMapping() {
        super();
    }
    
    public FieldMapping(String datatype, String fieldName, String modelFieldName, Direction direction, String columnVisibility) {
        super();
        this.datatype = datatype;
        this.fieldName = fieldName;
        this.modelFieldName = modelFieldName;
        this.direction = direction;
        this.columnVisibility = columnVisibility;
        
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
    
    @Override
    public String getProjection() {
        return getFieldName();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(columnVisibility).append(datatype).append(direction).append(fieldName).append(modelFieldName).toHashCode();
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
                        .append(fieldName, other.fieldName).append(modelFieldName, other.modelFieldName).isEquals();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("columnVisibility", columnVisibility).append("datatype", datatype).append("direction", direction)
                        .append("fieldName", fieldName).append("modelFieldName", modelFieldName).toString();
    }
    
    @Override
    public int compareTo(Mapping obj) {
        
        if (obj == null) {
            throw new IllegalArgumentException("can not compare null");
        }
        
        if (obj instanceof FieldMapping) {
            if (obj == this)
                return 0;
            
            return new CompareToBuilder().append(datatype, ((FieldMapping) obj).datatype).append(fieldName, ((FieldMapping) obj).fieldName)
                            .append(modelFieldName, ((FieldMapping) obj).modelFieldName).append(direction, ((FieldMapping) obj).direction)
                            .append(columnVisibility, ((FieldMapping) obj).columnVisibility).toComparison();
        }
        return 1;
    }
    
    @Override
    public void appendFields(StringBuilder builder) {
        builder.append("<td>").append(getColumnVisibility()).append("</td>");
        builder.append("<td>").append(getFieldName()).append("</td>");
        builder.append("<td>").append(getDatatype()).append("</td>");
        builder.append("<td>").append(getModelFieldName()).append("</td>");
        builder.append("<td>").append(getDirection()).append("</td>");
        builder.append("</tr>");
    }
    
}
