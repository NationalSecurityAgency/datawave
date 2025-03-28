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

@XmlRootElement(name = "ListUsersResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ListUsersResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElementWrapper(name = "Users")
    @XmlElement(name = "User")
    private List<String> users = null;
    
    public List<String> getUsers() {
        return users;
    }
    
    public void setUsers(List<String> users) {
        this.users = users;
    }
}
