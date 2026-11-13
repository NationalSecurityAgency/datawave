package datawave.webservice.dictionary.data;

import datawave.marking.Markings;
import datawave.webservice.query.result.event.HasMarkings;
import io.protostuff.Message;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class DescriptionBase<T> implements HasMarkings, Message<T> {

    protected String description;

    protected Markings<?> markings;

    public abstract String getDescription();

    public abstract void setDescription(String description);

}
