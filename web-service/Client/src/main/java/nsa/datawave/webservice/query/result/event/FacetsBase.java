package nsa.datawave.webservice.query.result.event;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import org.apache.accumulo.core.security.ColumnVisibility;

/**
 *
 */
@XmlAccessorType(XmlAccessType.NONE)
public abstract class FacetsBase implements HasMarkings {
    
    protected Map<String,String> markings;
    
    public abstract void setFields(List<? extends FieldCardinalityBase> fields);
    
    public abstract List<? extends FieldCardinalityBase> getFields();
    
    public abstract ColumnVisibility getColumnVisibility();
    
    public abstract void setColumnVisibility(ColumnVisibility columnVisibility);
    
    public abstract void setSizeInBytes(long sizeInBytes);
    
    public abstract long getSizeInBytes();
}
