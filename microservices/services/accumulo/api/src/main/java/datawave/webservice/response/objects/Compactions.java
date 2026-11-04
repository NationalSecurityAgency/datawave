package datawave.webservice.response.objects;

import java.io.Serializable;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

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
