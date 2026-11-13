package datawave.webservice.request.objects;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ValueReference {

    @XmlAttribute(required = true)
    private String id = null;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
