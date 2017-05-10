package datawave.webservice.response.objects;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Table implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement
    private String tablename = null;
    
    @XmlElement
    private Long tablets = null;
    
    @XmlElement
    private Long onlineTablets = null;
    
    @XmlElement
    private Long recs = null;
    
    @XmlElement
    private Long recsInMemory = null;
    
    @XmlElement
    private Double ingest = null;
    
    @XmlElement
    private Double query = null;
    
    @XmlElement(name = "majorCompactions")
    private Compaction majorCompactions = null;
    
    public String getTablename() {
        return tablename;
    }
    
    public Long getTablets() {
        return tablets;
    }
    
    public Long getRecs() {
        return recs;
    }
    
    public Long getRecsInMemory() {
        return recsInMemory;
    }
    
    public Compaction getMajorCompactions() {
        return majorCompactions;
    }
    
    public void setTablename(String tablename) {
        this.tablename = tablename;
    }
    
    public void setTablets(Long tablets) {
        this.tablets = tablets;
    }
    
    public void setRecs(Long recs) {
        this.recs = recs;
    }
    
    public void setRecsInMemory(Long recsInMemory) {
        this.recsInMemory = recsInMemory;
    }
    
    public void setMajorCompactions(Compaction majorCompactions) {
        this.majorCompactions = majorCompactions;
    }
    
    public Double getIngest() {
        return ingest;
    }
    
    public Double getQuery() {
        return query;
    }
    
    public void setIngest(Double ingest) {
        this.ingest = ingest;
    }
    
    public void setQuery(Double query) {
        this.query = query;
    }
    
    public Long getOnlineTablets() {
        return onlineTablets;
    }
    
    public void setOnlineTablets(Long onlineTablets) {
        this.onlineTablets = onlineTablets;
    }
    
}
