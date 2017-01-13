package nsa.datawave.webservice.response;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "VoidResponse")
public class VoidResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    public VoidResponse() {
        super();
    }
}
