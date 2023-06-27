package datawave.webservice.query.result.event;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.accumulo.core.security.ColumnVisibility;

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
