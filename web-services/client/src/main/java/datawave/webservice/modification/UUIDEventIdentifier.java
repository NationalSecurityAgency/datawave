package datawave.webservice.modification;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.builder.ToStringBuilder;

@XmlRootElement(name = "EventIdentifier")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class UUIDEventIdentifier implements Serializable {

    private static final long serialVersionUID = 1L;

    @XmlElement(name = "uuid", required = true)
    private String uuid = null;
    @XmlElement(name = "uuidType", required = false)
    private String uuidType = "UUID";

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuidType(String uuidType) {
        this.uuidType = uuidType;
    }

    public String getUuidType() {
        return uuidType;
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("uuid", uuid);
        tsb.append("uuidType", uuidType);
        return tsb.toString();
    }
}
