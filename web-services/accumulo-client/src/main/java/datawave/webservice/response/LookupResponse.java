package datawave.webservice.response;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.response.objects.Entry;
import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "LookupResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class LookupResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElementWrapper(name = "Entries")
    @XmlElement(name = "Entry")
    private List<Entry> entries = null;
    
    public List<Entry> getEntries() {
        return entries;
    }
    
    public void setEntries(List<Entry> entries) {
        this.entries = entries;
    }
}
