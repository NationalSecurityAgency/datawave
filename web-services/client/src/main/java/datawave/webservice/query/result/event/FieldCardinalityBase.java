package datawave.webservice.query.result.event;

import java.nio.charset.StandardCharsets;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.marking.Markings;

/**
 *
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultFieldCardinality.class)
public abstract class FieldCardinalityBase implements HasMarkings {

    protected Markings<?> markings;

    public abstract String getColumnVisibility();

    public abstract void setColumnVisibility(String columnVisibility);

    public void setColumnVisibility(ColumnVisibility columnVisibility) {
        String exprString = (columnVisibility == null) ? null : new String(columnVisibility.getExpression(), StandardCharsets.UTF_8);
        setColumnVisibility(exprString);
    }

    public void setColumnVisibility(AccessExpression accessExpression) {
        String exprString = (accessExpression == null) ? null : accessExpression.getExpression();
        setColumnVisibility(exprString);
    }

    public abstract String getField();

    public abstract void setField(String field);

    public abstract Long getCardinality();

    public abstract void setCardinality(Long cardinality);

    public abstract String getUpper();

    public abstract void setUpper(String upper);

    public abstract String getLower();

    public abstract void setLower(String lower);

    public abstract int hashCode();

    public abstract boolean equals(Object o);
}
