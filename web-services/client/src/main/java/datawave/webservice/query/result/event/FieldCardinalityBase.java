package datawave.webservice.query.result.event;

import java.nio.charset.StandardCharsets;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.marking.Markings;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlSeeAlso;

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
