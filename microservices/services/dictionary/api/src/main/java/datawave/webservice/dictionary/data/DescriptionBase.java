package datawave.webservice.dictionary.data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import datawave.marking.Markings;
import datawave.webservice.query.result.event.HasColumnVisibility;
import datawave.webservice.query.result.event.HasMarkings;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class DescriptionBase<T> implements HasMarkings, HasColumnVisibility, Message<T> {

    protected String description;

    /**
     * @deprecated use {@link #columnVisibility}
     */
    // TODO remove-accumulo-utils
    @Deprecated(forRemoval = true)
    protected Markings<?> markings;

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
