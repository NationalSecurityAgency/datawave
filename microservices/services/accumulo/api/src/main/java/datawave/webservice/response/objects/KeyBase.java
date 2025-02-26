package datawave.webservice.response.objects;

import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import datawave.webservice.query.result.event.HasMarkings;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultKey.class)
public abstract class KeyBase implements HasMarkings {
    
    protected Map<String,String> markings;
    
    public abstract String getRow();
    
    public abstract String getColFam();
    
    public abstract String getColQual();
    
    public abstract long getTimestamp();
    
    public abstract void setRow(String row);
    
    public abstract void setColFam(String colFam);
    
    public abstract void setColQual(String colQual);
    
    public abstract void setTimestamp(long timestamp);
    
}
