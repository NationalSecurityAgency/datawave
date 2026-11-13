package datawave.webservice.dictionary.data;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Multimap;

import datawave.webservice.result.BaseResponse;
import io.protostuff.Message;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlSeeAlso;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultFields.class)
public abstract class FieldsBase<T,DF extends DictionaryFieldBase<?,D>,D extends DescriptionBase> extends BaseResponse implements Message<T> {

    public abstract List<DF> getFields();

    public abstract void setFields(List<DF> fields);

    public abstract void setTotalResults(long totalResults);

    public abstract long getTotalResults();

    public abstract void setDescriptions(Multimap<Map.Entry<String,String>,D> descriptions);

}
