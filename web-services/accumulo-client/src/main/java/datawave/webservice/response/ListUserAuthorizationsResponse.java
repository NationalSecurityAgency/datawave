package datawave.webservice.response;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "ListUserAuthorizationsResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ListUserAuthorizationsResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElementWrapper(name = "UserAuthorizations")
    @XmlElement(name = "UserAuthorization")
    private List<String> userAuthorizations = null;
    
    public List<String> getUserAuthorizations() {
        return userAuthorizations;
    }
    
    public void setUserAuthorizations(List<String> userAuthorizations) {
        this.userAuthorizations = userAuthorizations;
    }
}
