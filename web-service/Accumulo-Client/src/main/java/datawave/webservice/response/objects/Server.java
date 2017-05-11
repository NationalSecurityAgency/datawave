package datawave.webservice.response.objects;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Server implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlAttribute
    private String id = null;
    
    @XmlElement
    private Long lastContact = null;
    
    @XmlElement
    private Compactions compactions = null;
    
    @XmlElement
    private Long tablets = null;
    
    @XmlElementWrapper(name = "loggers")
    @XmlElement(name = "logger")
    private List<String> loggers = null;
    
    public String getId() {
        return id;
    }
    
    public Long getLastContact() {
        return lastContact;
    }
    
    public Compactions getCompactions() {
        return compactions;
    }
    
    public Long getTablets() {
        return tablets;
    }
    
    public List<String> getLoggers() {
        return loggers;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public void setLastContact(Long lastContact) {
        this.lastContact = lastContact;
    }
    
    public void setCompactions(Compactions compactions) {
        this.compactions = compactions;
    }
    
    public void setTablets(Long tablets) {
        this.tablets = tablets;
    }
    
    public void setLoggers(List<String> loggers) {
        this.loggers = loggers;
    }
    
}
