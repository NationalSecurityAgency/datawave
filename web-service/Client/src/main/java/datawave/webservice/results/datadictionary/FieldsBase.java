package datawave.webservice.results.datadictionary;

import com.google.common.collect.Multimap;
import io.protostuff.Message;
import datawave.webservice.result.BaseResponse;

import java.util.List;
import java.util.Map;

/**

 */
public abstract class FieldsBase<T,DF extends DictionaryFieldBase<?,D>,D extends DescriptionBase> extends BaseResponse implements Message<T> {
    
    public abstract List<DF> getFields();
    
    public abstract void setFields(List<DF> fields);
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.result.TotalResultsAware#setTotalResults(long)
     */
    public abstract void setTotalResults(long totalResults);
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.result.TotalResultsAware#getTotalResults()
     */
    public abstract long getTotalResults();
    
    public abstract void setDescriptions(Multimap<Map.Entry<String,String>,D> descriptions);
    
}
