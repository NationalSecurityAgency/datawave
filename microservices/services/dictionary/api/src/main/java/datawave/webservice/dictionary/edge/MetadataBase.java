package datawave.webservice.dictionary.edge;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import com.google.common.collect.Maps;

import datawave.webservice.query.result.event.HasMarkings;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultMetadata.class)
public abstract class MetadataBase<T> implements HasMarkings, Message<T> {
    
    protected transient Map<String,String> markings;
    
    public Map<String,String> getMarkings() {
        assureMarkings();
        return markings;
    }
    
    protected void assureMarkings() {
        if (this.markings == null)
            this.markings = Maps.newHashMap();
    }
    
    public void setMarkings(Map<String,String> markings) {
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
