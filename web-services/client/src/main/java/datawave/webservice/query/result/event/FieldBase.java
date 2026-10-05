package datawave.webservice.query.result.event;

import java.nio.charset.StandardCharsets;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.marking.Markings;
import datawave.webservice.query.util.TypedValue;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class FieldBase<T> implements HasMarkings, Message<T> {

    protected transient Markings<?> markings;

    public abstract Long getTimestamp();

    public abstract String getValueString();

    public abstract TypedValue getTypedValue();

    public abstract Object getValueOfTypedValue();

    public abstract void setTimestamp(Long timestamp);

    public abstract void setValue(Object value);

    public abstract String getName();

    public abstract void setName(String name);

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

}
