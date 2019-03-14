package datawave.webservice.results.datadictionary;

import io.protostuff.Message;
import datawave.webservice.HtmlProvider;
import datawave.webservice.query.result.metadata.MetadataFieldBase;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.TotalResultsAware;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.util.Collection;
import java.util.List;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultDataDictionary.class)
public abstract class DataDictionaryBase<T,M extends MetadataFieldBase> extends BaseResponse implements TotalResultsAware, Message<T>, HtmlProvider {
    
    public abstract List<M> getFields();
    
    public abstract void setFields(Collection<M> fields);
    
    public abstract void setTotalResults(long totalResults);
    
    public abstract long getTotalResults();
    
    public abstract String getTitle();
    
    public abstract String getHeadContent();
    
    public abstract String getPageHeader();
    
    public abstract String getMainContent();
}
