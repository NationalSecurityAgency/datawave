package datawave.webservice.response;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import datawave.webservice.response.objects.UserPermissions;
import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "ListUserPermissionsResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ListUserPermissionsResponse extends BaseResponse {

    private static final long serialVersionUID = 1L;

    @XmlElement(name = "Permissions")
    private UserPermissions userPermissions = null;

    public UserPermissions getUserPermissions() {
        return userPermissions;
    }

    public void setUserPermissions(UserPermissions userPermissions) {
        this.userPermissions = userPermissions;
    }

}
