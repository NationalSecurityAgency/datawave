package datawave.webservice.dictionary.edge;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import datawave.marking.Markings;
import datawave.webservice.query.result.event.HasMarkings;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultMetadata.class)
public abstract class MetadataBase<T> implements HasMarkings, Message<T> {

    protected transient Markings<?> markings;

    public Markings<?> getMarkings() {
        return markings;
    }

    public void setMarkings(Markings<?> markings) {
        this.markings = markings;
    }

    public abstract String getEdgeType();

    public abstract void setEdgeType(String edgeType);

    public abstract String getEdgeRelationship();

    public abstract void setEdgeRelationship(String edgeRelationship);

    public abstract String getEdgeAttribute1Source();

    public abstract void setEdgeAttribute1Source(String edgeAttribute1Source);

    public abstract String getStartDate();

    public abstract void setStartDate(String startDate);

    public abstract String getLastUpdated();

    public abstract void setLastUpdated(String lastUpdated);

    public abstract boolean hasEdgeAttribute1Source();

    public abstract List<EventField> getEventFields();

    public abstract void setEventFields(List<EventField> eventFields);

    public abstract String getJexlPrecondition();

}
