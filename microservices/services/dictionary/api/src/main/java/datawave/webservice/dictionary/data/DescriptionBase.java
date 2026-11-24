package datawave.webservice.dictionary.data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import datawave.webservice.query.result.event.HasColumnVisibility;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class DescriptionBase<T> implements HasColumnVisibility, Message<T> {

    protected String description;

    protected String columnVisibility;

    @Override
    public String getColumnVisibility() {
        return columnVisibility;
    }

    @Override
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public abstract String getDescription();

    public abstract void setDescription(String description);

}
