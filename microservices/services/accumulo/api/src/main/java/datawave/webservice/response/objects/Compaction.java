package datawave.webservice.response.objects;

import java.io.Serializable;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Compaction implements Serializable {

    private static final long serialVersionUID = 1L;

    @XmlElement
    private Long running = null;

    @XmlElement
    private Long queued = null;

    public Long getRunning() {
        return running;
    }

    public Long getQueued() {
        return queued;
    }

    public void setRunning(Long running) {
        this.running = running;
    }

    public void setQueued(Long queued) {
        this.queued = queued;
    }
}
