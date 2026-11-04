package datawave.webservice.response;

import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

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
