package datawave.webservice.dictionary.data;

import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultDictionaryField.class)
public abstract class DictionaryFieldBase<T,D extends DescriptionBase> implements Message<T> {
    
    public abstract String getFieldName();
    
    public abstract void setFieldName(String fieldName);
    
    public abstract String getDatatype();
    
    public abstract void setDatatype(String datatype);
    
    public abstract Set<D> getDescriptions();
    
    public abstract void addDescription(D description);
    
    public abstract void setDescriptions(Set<D> descriptions);
    
}
