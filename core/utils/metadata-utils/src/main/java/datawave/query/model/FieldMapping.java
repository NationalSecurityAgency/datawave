package datawave.query.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@XmlAccessorType(XmlAccessType.NONE)
public class FieldMapping implements Serializable, Comparable<FieldMapping> {
    
    private static final long serialVersionUID = 1L;
    @XmlAttribute(name = "datatype")
    private String datatype = null;
    
    @XmlAttribute(name = "fieldName")
    private String fieldName = null;
    
    @XmlAttribute(name = "modelFieldName", required = true)
    private String modelFieldName = null;
    
    @XmlAttribute(name = "direction")
    private Direction direction = null;
    
    @XmlAttribute(name = "columnVisibility", required = true)
    private String columnVisibility = null;
    
    @XmlElementWrapper(name = "attributes")
    @XmlElement(name = "attribute")
    private TreeSet<String> attributes = new TreeSet<>();
    
    public FieldMapping() {
        super();
    }
    
    public FieldMapping(String datatype, String fieldName, String modelFieldName, Direction direction, String columnVisibility, Collection<String> attributes) {
        super();
        this.datatype = datatype;
        this.fieldName = fieldName;
        this.modelFieldName = modelFieldName;
        this.direction = direction;
        this.columnVisibility = columnVisibility;
        this.attributes.addAll(attributes);
        validate();
    }
    
    public FieldMapping(String datatype, String fieldName, String modelFieldName, Direction direction, String columnVisibility) {
        this(datatype, fieldName, modelFieldName, direction, columnVisibility, Collections.emptyList());
    }
    
    public FieldMapping(String modelFieldName, String columnVisibility, Collection<String> attributes) {
        this(null, null, modelFieldName, null, columnVisibility, attributes);
    }
    
    public void validate() {
        // this mapping either represents attributes of the model field, or a field mapping. Cannot be both
        // if we have a direction, then we have a field mapping
        if (direction != null) {
            // note datatype can be null
            if (fieldName == null || modelFieldName == null || columnVisibility == null) {
                throw new IllegalArgumentException("Cannot have a model mapping with without all members: " + this);
            }
            // If this is a forward mapping, it's possible that a regex pattern is supplied for the field name. Verify that the field name compiles.
            if (direction == Direction.FORWARD) {
                try {
                    Pattern.compile(fieldName);
                } catch (PatternSyntaxException e) {
                    throw new IllegalArgumentException("Invalid regex pattern supplied for field name: " + fieldName, e);
                }
            }
        }
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
    
    public Set<String> getAttributes() {
        return Collections.unmodifiableSet(attributes);
    }
    
    public void setAttributes(Collection<String> attributes) {
        this.attributes.clear();
        if (attributes != null) {
            this.attributes.addAll(attributes);
        }
    }
    
    public void addAttribute(String attribute) {
        this.attributes.add(attribute);
    }
    
    /**
     * We are a field mapping IFF we have a direction
     * 
     * @return true if this is a field mapping (not just model field attributes)
     */
    public boolean isFieldMapping() {
        return direction != null;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(columnVisibility).append(datatype).append(direction).append(fieldName).append(modelFieldName)
                        .append(attributes).toHashCode();
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
                        .append(fieldName, other.fieldName).append(modelFieldName, other.modelFieldName).append(attributes, other.attributes).isEquals();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("columnVisibility", columnVisibility).append("datatype", datatype).append("direction", direction)
                        .append("fieldName", fieldName).append("modelFieldName", modelFieldName).append("attributes", attributes).toString();
    }
    
    @Override
    public int compareTo(FieldMapping obj) {
        
        if (obj == null) {
            throw new IllegalArgumentException("can not compare null");
        }
        
        if (obj == this)
            return 0;
        
        return new CompareToBuilder().append(datatype, obj.datatype).append(fieldName, obj.fieldName).append(modelFieldName, obj.modelFieldName)
                        .append(direction, obj.direction).append(columnVisibility, obj.columnVisibility).append(attributes.size(), obj.attributes.size())
                        .append(attributes.toString(), obj.attributes.toString()).toComparison();
    }
    
}
