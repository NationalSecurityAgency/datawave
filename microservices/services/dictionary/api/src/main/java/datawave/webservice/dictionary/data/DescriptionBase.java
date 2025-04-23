package datawave.webservice.dictionary.data;

import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import datawave.webservice.query.result.event.HasMarkings;
import datawave.webservice.xml.util.StringMapAdapter;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class DescriptionBase<T> implements HasMarkings, Message<T> {
    
    @XmlElement(name = "description")
    protected String description;
    
    @XmlElement(name = "markings")
    @XmlJavaTypeAdapter(StringMapAdapter.class)
    protected Map<String,String> markings;
    
    public abstract String getDescription();
    
    public abstract void setDescription(String description);
    
}
