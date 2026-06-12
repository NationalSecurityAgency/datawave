package datawave.webservice.dictionary.data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import datawave.marking.Markings;
import datawave.webservice.query.result.event.HasMarkings;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class DescriptionBase<T> implements HasMarkings, Message<T> {

    protected String description;

    protected Markings<?> markings;

    public abstract String getDescription();

    public abstract void setDescription(String description);

}
