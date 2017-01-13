package nsa.datawave.webservice.results.datadictionary;

import io.protostuff.Message;
import nsa.datawave.webservice.HtmlProvider;
import nsa.datawave.webservice.query.result.metadata.MetadataFieldBase;
import nsa.datawave.webservice.result.BaseResponse;
import nsa.datawave.webservice.result.TotalResultsAware;

import java.util.Collection;
import java.util.List;

/**
  */
public abstract class DataDictionaryBase<T,M extends MetadataFieldBase> extends BaseResponse implements TotalResultsAware, Message<T>, HtmlProvider {
    
    public abstract List<M> getFields();
    
    public abstract void setFields(Collection<M> fields);
    
    public abstract void setTotalResults(long totalResults);
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.result.TotalResultsAware#getTotalResults()
     */
    public abstract long getTotalResults();
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getTitle()
     */
    public abstract String getTitle();
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getHeadContent()
     */
    public abstract String getHeadContent();
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getPageHeader()
     */
    public abstract String getPageHeader();
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.webservice.HtmlProvider#getMainContent()
     */
    public abstract String getMainContent();
}
