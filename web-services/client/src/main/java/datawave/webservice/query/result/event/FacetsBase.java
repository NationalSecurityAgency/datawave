package datawave.webservice.query.result.event;

import org.apache.accumulo.core.security.ColumnVisibility;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlSeeAlso;
import jakarta.xml.bind.annotation.XmlTransient;
import java.util.List;
import java.util.Map;

/**
 *
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultFacets.class)
public abstract class FacetsBase implements HasMarkings {
    
    protected Map<String,String> markings;
    
    @XmlElementWrapper(name = "Fields")
    @XmlElement(name = "Field")
    protected List<FieldCardinalityBase> fields = null;
    
    @XmlTransient
    protected ColumnVisibility columnVisibility;
    
    public void setFields(List<? extends FieldCardinalityBase> fields) {
        this.fields = (List<FieldCardinalityBase>) fields;
    }
    
    public List<? extends FieldCardinalityBase> getFields() {
        return fields;
    }
    
    public ColumnVisibility getColumnVisibility() {
        return columnVisibility;
    }
    
    public void setColumnVisibility(ColumnVisibility columnVisibility) {
        this.columnVisibility = columnVisibility;
    }
    
    public abstract void setSizeInBytes(long sizeInBytes);
    
    public abstract long getSizeInBytes();
}
