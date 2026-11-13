package datawave.webservice.response;

import java.util.List;

import datawave.webservice.result.BaseResponse;
import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

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
