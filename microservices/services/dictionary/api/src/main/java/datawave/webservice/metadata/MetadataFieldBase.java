package datawave.webservice.metadata;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import datawave.webservice.dictionary.data.DescriptionBase;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultMetadataField.class)
public abstract class MetadataFieldBase<T,D extends DescriptionBase> implements Serializable, Message<T> {
    private static final long serialVersionUID = -5902264162414621612L;
    
    public abstract String getFieldName();
    
    public abstract void setFieldName(String fieldName);
    
    public abstract String getInternalFieldName();
    
    public abstract void setInternalFieldName(String internalFieldName);
    
    public abstract String getDataType();
    
    public abstract void setDataType(String dataType);
    
    public abstract Boolean isIndexOnly();
    
    public abstract void setIndexOnly(Boolean indexOnly);
    
    public abstract Boolean isForwardIndexed();
    
    public abstract void setForwardIndexed(Boolean indexed);
    
    public abstract Boolean isReverseIndexed();
    
    public abstract void setReverseIndexed(Boolean reverseIndexed);
    
    public abstract Boolean isNormalized();
    
    public abstract void setNormalized(Boolean normalized);
    
    public abstract void addType(String type);
    
    public abstract List<String> getTypes();
    
    public abstract void setTypes(List<String> types);
    
    public abstract Set<D> getDescriptions();
    
    public abstract void setDescription(Collection<D> descriptions);
    
    public abstract void setLastUpdated(String lastUpdated);
    
    public abstract String getLastUpdated();
    
}
