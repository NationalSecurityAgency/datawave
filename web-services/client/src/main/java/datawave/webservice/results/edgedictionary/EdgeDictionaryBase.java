package datawave.webservice.results.edgedictionary;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import io.protostuff.Message;
import datawave.webservice.result.BaseResponse;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultEdgeDictionary.class)
public abstract class EdgeDictionaryBase<T,F extends MetadataBase<F>> extends BaseResponse implements Message<T> {
    
    private static final long serialVersionUID = 1L;
    
    public abstract List<? extends MetadataBase<F>> getMetadataList();
    
    public abstract void setTotalResults(long totalResults);
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.result.TotalResultsAware#getTotalResults()
     */
    public abstract long getTotalResults();
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getTitle()
     */
    
    public abstract String getTitle();
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getHeadContent()
     */
    
    public abstract String getHeadContent();
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getPageHeader()
     */
    public abstract String getPageHeader();
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getMainContent()
     */
    
    public abstract String getMainContent();
    
}
