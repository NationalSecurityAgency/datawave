package datawave.webservice.results.datadictionary;

import io.protostuff.Message;
import datawave.webservice.query.result.event.HasMarkings;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import java.util.Map;

/**

 */
@XmlAccessorType(XmlAccessType.NONE)
public abstract class DescriptionBase<T> implements HasMarkings, Message<T> {
    
    protected transient Map<String,String> markings;
    
    public abstract String getDescription();
    
    public abstract void setDescription(String description);
    
}
