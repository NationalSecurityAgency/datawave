package datawave.webservice.dictionary.data;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import com.google.common.collect.Multimap;

import datawave.webservice.result.BaseResponse;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultFields.class)
public abstract class FieldsBase<T,DF extends DictionaryFieldBase<?,D>,D extends DescriptionBase> extends BaseResponse implements Message<T> {
    
    public abstract List<DF> getFields();
    
    public abstract void setFields(List<DF> fields);
    
    public abstract void setTotalResults(long totalResults);
    
    public abstract long getTotalResults();
    
    public abstract void setDescriptions(Multimap<Map.Entry<String,String>,D> descriptions);
    
}
