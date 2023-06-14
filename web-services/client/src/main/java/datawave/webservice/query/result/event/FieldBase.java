package datawave.webservice.query.result.event;

import java.util.Map;

import com.google.common.base.Charsets;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import datawave.webservice.query.util.TypedValue;

import io.protostuff.Message;
import org.apache.accumulo.core.security.ColumnVisibility;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultField.class)
public abstract class FieldBase<T> implements HasMarkings, Message<T> {

    protected transient Map<String,String> markings;

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
        String cvString = (columnVisibility == null) ? null : new String(columnVisibility.getExpression(), Charsets.UTF_8);
        setColumnVisibility(cvString);
    }

}
