package nsa.datawave.webservice.response;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import nsa.datawave.webservice.response.objects.Visibility;

@XmlRootElement(name = "ValidateVisibilityResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ValidateVisibilityResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    public ValidateVisibilityResponse() {
        super();
    }
    
    @XmlElementWrapper(name = "Visibilities")
    @XmlElement(name = "Visibility")
    List<Visibility> visibilityList = null;
    
    public List<Visibility> getVisibilityList() {
        return visibilityList;
    }
    
    public void setVisibilityList(List<Visibility> visibilityList) {
        this.visibilityList = visibilityList;
    }
}
