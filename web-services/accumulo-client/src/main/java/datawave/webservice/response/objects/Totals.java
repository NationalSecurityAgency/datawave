package datawave.webservice.response.objects;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Totals implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement
    private Double ingestrate = null;
    
    @XmlElement
    private Double queryrate = null;
    
    @XmlElement
    private Double numentries = null;
    
    @XmlElement
    private Double diskrate = null;
    
    public Double getIngestrate() {
        return ingestrate;
    }
    
    public Double getQueryrate() {
        return queryrate;
    }
    
    public Double getNumentries() {
        return numentries;
    }
    
    public Double getDiskrate() {
        return diskrate;
    }
    
    public void setIngestrate(Double ingestrate) {
        this.ingestrate = ingestrate;
    }
    
    public void setQueryrate(Double queryrate) {
        this.queryrate = queryrate;
    }
    
    public void setNumentries(Double numentries) {
        this.numentries = numentries;
    }
    
    public void setDiskrate(Double diskrate) {
        this.diskrate = diskrate;
    }
}
