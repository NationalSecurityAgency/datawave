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
