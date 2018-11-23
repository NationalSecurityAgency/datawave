package datawave.webservice.response;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.response.objects.TableProperty;
import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "TablePropertiesResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class TablePropertiesResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElementWrapper(name = "Properties")
    @XmlElement(name = "Property")
    private List<TableProperty> properties = null;
    
    public List<TableProperty> getProperties() {
        return properties;
    }
    
    public void setProperties(List<TableProperty> properties) {
        this.properties = properties;
    }
}
