package datawave.webservice.modification;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@XmlRootElement(name = "DefaultUUIDModificationRequest")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultUUIDModificationRequest extends ModificationRequestBase implements Serializable {

    private static final long serialVersionUID = 3L;

    @XmlElementWrapper(name = "Events", required = true)
    @XmlElement(name = "Event", required = true)
    private List<ModificationEvent> events = null;

    public void setEvents(List<ModificationEvent> events) {
        this.events = events;
    }

    public List<ModificationEvent> getEvents() {
        return events;
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("Events", events);
        return tsb.toString();
    }

    @Override
    public Map<String,List<String>> toMap() {
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        p.putAll(super.toMap());
        if (this.events != null) {
            for (ModificationEvent e : events) {
                p.add("Events", e.toString());
            }
        }
        return p;
    }
}
