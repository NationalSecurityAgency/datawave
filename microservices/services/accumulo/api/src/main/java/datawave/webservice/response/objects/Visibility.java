package datawave.webservice.response.objects;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import datawave.webservice.xml.util.StringMapAdapter;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Visibility {
    
    @XmlAttribute(name = "valid")
    private Boolean valid;
    
    @XmlElement(name = "markings")
    @XmlJavaTypeAdapter(StringMapAdapter.class)
    private HashMap<String,String> markings;
    
    @XmlAttribute(name = "visibility")
    private String visibility;
    
    public Boolean getValid() {
        return valid;
    }
    
    public void setValid(Boolean valid) {
        this.valid = valid;
    }
    
    public String getVisibility() {
        return visibility;
    }
    
    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }
    
    public Map<String,String> getMarkings() {
        return markings;
    }
    
    public void setMarkings(Map<String,String> markings) {
        if (this.markings == null) {
            this.markings = new HashMap<String,String>();
        }
        this.markings.clear();
        this.markings.putAll(markings);
    }
    
}
