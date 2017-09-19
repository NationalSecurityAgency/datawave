package datawave.webservice.response.objects;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "compactions")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Compactions implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement
    private Compaction major = null;
    
    @XmlElement
    private Compaction minor = null;
    
    public Compaction getMajor() {
        return major;
    }
    
    public Compaction getMinor() {
        return minor;
    }
    
    public void setMajor(Compaction major) {
        this.major = major;
    }
    
    public void setMinor(Compaction minor) {
        this.minor = minor;
    }
}
