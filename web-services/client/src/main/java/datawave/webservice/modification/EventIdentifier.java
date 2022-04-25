package datawave.webservice.modification;

import java.io.Serializable;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;

@XmlRootElement(name = "EventIdentifier")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class EventIdentifier implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "shardId", required = true)
    private String shardId = null;
    @XmlElement(name = "datatype", required = true)
    private String datatype = null;
    @XmlElement(name = "eventUid", required = true)
    private String eventUid = null;
    
    public String getShardId() {
        return shardId;
    }
    
    public void setShardId(String shardId) {
        this.shardId = shardId;
    }
    
    public String getDatatype() {
        return datatype;
    }
    
    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }
    
    public String getEventUid() {
        return eventUid;
    }
    
    public void setEventUid(String eventUid) {
        this.eventUid = eventUid;
    }
    
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("shardId", shardId);
        tsb.append("datatype", datatype);
        tsb.append("eventUid", eventUid);
        return tsb.toString();
    }
}
