package datawave.webservice.query.result.event;

import java.util.List;

import org.apache.accumulo.access.AccessExpression;

import datawave.marking.Markings;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlSeeAlso;
import jakarta.xml.bind.annotation.XmlTransient;

/**
 *
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultFacets.class)
public abstract class FacetsBase implements HasMarkings {

    protected Markings<?> markings;

    @XmlElementWrapper(name = "Fields")
    @XmlElement(name = "Field")
    protected List<FieldCardinalityBase> fields = null;

    @XmlTransient
    protected AccessExpression accessExpression;

    @SuppressWarnings("unchecked")
    public void setFields(List<? extends FieldCardinalityBase> fields) {
        this.fields = (List<FieldCardinalityBase>) fields;
    }

    public List<? extends FieldCardinalityBase> getFields() {
        return fields;
    }

    public AccessExpression getAccessExpression() {
        return accessExpression;
    }

    public void setAccessExpression(AccessExpression accessExpression) {
        this.accessExpression = accessExpression;
    }

    public abstract void setSizeInBytes(long sizeInBytes);

    public abstract long getSizeInBytes();
}
