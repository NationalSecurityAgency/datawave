package datawave.webservice.query.result.event;

import org.apache.accumulo.core.security.ColumnVisibility;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;
import java.util.List;
import java.util.Map;

/**
 *
 */
@XmlAccessorType(XmlAccessType.NONE)
public abstract class FacetsBase implements HasMarkings {
    
    protected Map<String,String> markings;
    
    @XmlElementWrapper(name = "Fields")
    @XmlElement(name = "Field")
    protected List<FieldCardinality> fields = null;
    
    @XmlTransient
    protected ColumnVisibility columnVisibility;
    
    public void setFields(List<? extends FieldCardinalityBase> fields) {
        this.fields = (List<FieldCardinality>) fields;
    }
    
    public List<FieldCardinality> getFields() {
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
